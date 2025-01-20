import json
import os
from contextlib import asynccontextmanager
from datetime import datetime

import neulander_core.models.medcat as m
from azure.storage.blob import (
    BlobClient,
    ContainerClient,
)
from dotenv import find_dotenv, load_dotenv
from faststream import Context, ContextRepo, Logger
from faststream.asgi import AsgiFastStream, make_ping_asgi
from faststream.rabbit import RabbitBroker, RabbitMessage, RabbitQueue
from medcat.cat import CAT
from pydantic import AmqpDsn, BaseModel, FilePath, HttpUrl

load_dotenv(find_dotenv())


class MedCATConfig(BaseModel):
    medcat_model_path: HttpUrl | FilePath
    rabbitmq_url: AmqpDsn


config = MedCATConfig(
    medcat_model_path=os.environ["MEDCAT_MODEL_PATH"],
    rabbitmq_url=os.environ["RABBITMQ_URL"],
)

broker = RabbitBroker(url=config.rabbitmq_url, max_consumers=1)
qin = RabbitQueue("q-medcat_14-in", durable=True)
qout = RabbitQueue(name="q-medcat_14-out", durable=True)


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
    # Load medcat model here. This can take a while
    try:
        logger.info("Loading MedCAT models.")

        if type(config.medcat_model_path) is HttpUrl:
            # get model and load it here
            pass
        else:
            # cat = CAT.load_model_pack(zip_path=str(config.medcat_model_path))
            cat = DummyCAT()

            context.set_global("cat", cat)
        yield
    except KeyError as e:
        logger.critical("MEDCAT_MODEL_PATH environment variable must be set.", *e.args)
        raise
    finally:
        # do some closing actions here
        pass


def get_ts():
    return datetime.isoformat(datetime.now())


async def write_blob(doc_out: m.DocOut, sas_url: HttpUrl):
    c = ContainerClient.from_container_url(sas_url)

    now = datetime.now()

    blob_path = f"{now.year}/{now.month}/{now.day}/{now.hour}/{doc_out.id}"

    b = c.upload_blob(name=blob_path, data=doc_out.json(), overwrite=True)

    return b.blob_name


async def read_blob(sas_url: HttpUrl):
    bc = BlobClient.from_blob_url(sas_url)
    # ToDo: Handle timeout if blob storage is not accessible
    blob = bc.download_blob(timeout=10)
    blob_content = blob.content_as_text()
    return json.loads(blob_content)


@broker.publisher(queue=qout)
@broker.subscriber(
    queue=qin,
    no_ack=True,
    title="MedCAT Annotation Worker",
)
async def processor(
    body: dict,
    msg: RabbitMessage,
    logger: Logger,
    cat: CAT = Context("cat"),
):
    # If body is encrypted, decrypt here.
    # eg. doc: DocIn = decrypt(body)
    # try:
    doc_meta = dict()

    doc_meta["start"] = get_ts()

    doc_in = m.DocIn(**body)

    sas_url = doc_in.src

    doc = await read_blob(sas_url=sas_url)
    doc_meta["Blob dowload complete"] = get_ts()

    result = cat.get_entities(doc["doc_text"])

    doc_meta["Annotation complete"] = get_ts()

    entities = {k: m.MedcatOutput.validate(v) for k, v in result["entities"].items()}

    doc_meta["length"] = len(doc["doc_text"])
    doc_out = m.DocOut(
        id=doc_in.id,
        text=doc["doc_text"],
        entities=entities,
        model_meta=cat.get_model_card(as_dict=True),
        doc_meta=doc_meta,
    )

    blob_name = await write_blob(doc_out=doc_out, sas_url=doc_in.dest)

    doc_meta["Output blob uploaded"] = get_ts()

    await msg.ack()

    return {
        "id": doc_out.id,
        "blob_name": blob_name,
        "correlation_id": msg.correlation_id,
        "message_id": msg.message_id,
    }


# except Exception as e:
#     logger.error(e)
#     return {"status": "error"}

#     await msg.ack()

# finally:
#     pass


app = AsgiFastStream(
    broker,
    asgi_routes=[
        ("/health", make_ping_asgi(broker, timeout=5.0)),
        ("/liveness", make_ping_asgi(broker, timeout=5.0)),
    ],
    asyncapi_path="/docs",
    lifespan=lifespan,
)
