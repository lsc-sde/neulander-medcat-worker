import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime

import neulander_core.schema.medcat_schema as m
from dotenv import find_dotenv, load_dotenv
from faststream import Context, ContextRepo, Logger
from faststream.asgi import AsgiFastStream, make_ping_asgi
from faststream.rabbit import RabbitBroker, RabbitMessage
from medcat.cat import CAT
from neulander_core.config import WorkerQueues
from neulander_core.config import settings as cfg
from neulander_core.crud import AzureBlobStorage
from neulander_core.schema.core import AzureBlobDocIn, DocOut

load_dotenv(find_dotenv())


__version__ = "0.1.0"

medcat_model_path = os.getenv("MEDCAT_MODEL_PATH", "")
dummy_mode = os.getenv("DUMMY_MODE")

# Setup Rabbit Broker
broker = RabbitBroker(url=cfg.rabbitmq_connection_string, max_consumers=1)

# Setup RabbitMQ Queues
WORKER_NAME = os.getenv("WORKER_NAME", "medcat_14")
queues = WorkerQueues(worker_name=WORKER_NAME)

##############################################################################
# Utility functions
##############################################################################


def get_ts():
    return datetime.isoformat(datetime.now())


##############################################################################
# Utility functions
##############################################################################


class DummyCAT:
    def __init__(self):
        pass

    def get_entities(self, doc):
        return {
            "entities": {
                "3": {
                    "pretty_name": "Jones",
                    "cui": "261864007",
                    "type_ids": ["7882689"],
                    "types": [""],
                    "source_value": "Jones",
                    "detected_name": "jones",
                    "acc": 1.0,
                    "context_similarity": 1.0,
                    "start": 26,
                    "end": 31,
                    "icd10": [],
                    "ontologies": ["20220803_SNOMED_UK_CLINICAL_EXT"],
                    "snomed": [],
                    "id": 3,
                    "meta_anns": {
                        "Presence": {
                            "value": "True",
                            "confidence": 0.9999998807907104,
                            "name": "Presence",
                        },
                        "Subject": {
                            "value": "Patient",
                            "confidence": 0.9999642372131348,
                            "name": "Subject",
                        },
                        "Time": {
                            "value": "Recent",
                            "confidence": 0.9997757077217102,
                            "name": "Time",
                        },
                    },
                },
                "4": {
                    "pretty_name": "Neurology",
                    "cui": "394591006",
                    "type_ids": ["7882689"],
                    "types": [""],
                    "source_value": "neurology",
                    "detected_name": "neurology",
                    "acc": 1.0,
                    "context_similarity": 1.0,
                    "start": 39,
                    "end": 48,
                    "icd10": [],
                    "ontologies": ["20220803_SNOMED_UK_CLINICAL_EXT"],
                    "snomed": [],
                    "id": 4,
                    "meta_anns": {
                        "Presence": {
                            "value": "True",
                            "confidence": 0.9999997615814209,
                            "name": "Presence",
                        },
                        "Subject": {
                            "value": "Patient",
                            "confidence": 0.9833911657333374,
                            "name": "Subject",
                        },
                        "Time": {
                            "value": "Recent",
                            "confidence": 0.9999591112136841,
                            "name": "Time",
                        },
                    },
                },
            },
            "tokens": [],
        }

    def get_model_card(self, as_dict: bool = True):
        return {"model_name": "Dummy"}


# Setup lifespan events
@asynccontextmanager
async def lifespan(context: ContextRepo, logger: Logger):
    # Check if rabbitmq broker is available
    # If not, try 2 more times before raising an exception
    logger.info("Setting up lifespan events.")
    retries = 3
    for attempt in range(retries):
        try:
            await broker.connect()
            logger.info("Successfully connected to RabbitMQ.")
            break
        except Exception as e:
            if attempt < retries - 1:
                backoff = 5 + attempt * 5
                logger.warning(
                    f"Retrying connection to RabbitMQ ({attempt + 1}/{retries}) in {backoff} seconds: {e}"
                )
                await asyncio.sleep(backoff)  # exponential(ish) backoff before retrying
            else:
                logger.error(
                    f"Failed to connect to RabbitMQ after {retries} attempts: {e}"
                )
                raise

    # Load medcat model here. This can take a while
    # Alternatively use a dummy class that mocks MedCAT during development
    try:
        if dummy_mode == "true":
            logger.info("Using DummyCAT for testing only.")
            cat = DummyCAT()
        else:
            logger.info(f"Loading MedCAT models from {medcat_model_path}")
            cat = CAT.load_model_pack(zip_path=medcat_model_path)
            logger.info("Finished loading MedCAT models.")

        context.set_global("cat", cat)

        yield

    except Exception as e:
        logger.critical("Error loading medcat models", *e.args)
        raise

    finally:
        # do some closing actions here
        pass


@broker.publisher(queue=queues.qout)
@broker.subscriber(
    queue=queues.qin,
    no_ack=True,
    title="MedCAT Annotation Worker",
)
async def process_message(
    body: dict,
    msg: RabbitMessage,
    logger: Logger,
    cat: CAT = Context("cat"),
):
    # ToDo: This try block is probably unncessary as FastAPI has already validated the request

    try:
        # If body is encrypted, decrypt here.
        # eg. doc: DocIn = decrypt(body)

        docin = AzureBlobDocIn(**body)

    except Exception as e:
        logger.error("Error parsing message - {body}. {e}")
        out = {
            "error": str(e),
            "correlation_id": msg.correlation_id,
            "message_id": msg.message_id,
        }
        response = await broker.publish(
            message=out,  # type: ignore
            correlation_id=msg.correlation_id,
            message_id=msg.message_id,
            queue=queues.qerr,
        )
        await msg.reject()
        return out

    try:
        docin.docmeta["start_work"] = get_ts()  # type: ignore

        logger.info(f"Reading blob {docin.docid}")

        doctext = await AzureBlobStorage(docin.src.unicode_string()).read(docin.docid)
        # Medcat worker expects the blob to simply contain the document text
        # If there is additional preprocessing required, that should happen here.
        # Consider using docin.docext (eg. rtf, pdf, etc. ) to do this.
        doctext = doctext.decode()

        docin.docmeta["doc_length"] = len(doctext)  # type: ignore
        docin.docmeta["blob_downloaded"] = get_ts()  # type: ignore

        result = cat.get_entities(doctext)
        entities = {
            k: m.MedcatEntity.model_validate(v) for k, v in result["entities"].items()
        }
        docin.docmeta["annotation_completed"] = get_ts()  # type: ignore

        medcatoutput = m.MedcatOutput(
            docid=docin.docid,
            text=doctext,
            entities=entities,
            docmeta=docin.docmeta,
            modelmeta=WORKER_NAME,
        )
        # Add docext to this.
        docout = DocOut(docid=docin.docid, response=medcatoutput.model_dump())

        response = await AzureBlobStorage(docin.dest.unicode_string()).write(
            blob_name=f"{docin.docname}.json", data=docout.model_dump_json()
        )

        docin.docmeta["blob_uploaded"] = get_ts()  # type: ignore

        await msg.ack()

        return {
            "docid": docin.docid,
            "docname": response.blob_name,
            "correlation_id": msg.correlation_id,
            "message_id": msg.message_id,
        }

    except Exception as e:
        logger.error(f"Error processing document {docin.docid}: {e}")

        out = {
            "docid": docin.docid,
            "docmeta": docin.docmeta,
            "modelmeta": WORKER_NAME,
            "error": str(e),
            "correlation_id": msg.correlation_id,
            "message_id": msg.message_id,
        }

        response = await AzureBlobStorage(docin.dest.unicode_string()).write(
            blob_name=f"{docin.docname}.err", data=json.dumps(out)
        )
        await msg.ack()

    finally:
        pass


app = AsgiFastStream(
    broker,
    asgi_routes=[
        ("/health", make_ping_asgi(broker, timeout=5.0)),
        ("/liveness", make_ping_asgi(broker, timeout=5.0)),
    ],
    asyncapi_path="/docs",
    lifespan=lifespan,
)
