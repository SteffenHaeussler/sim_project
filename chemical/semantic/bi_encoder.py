from typing import List

import ray
import torch
from ray import serve
from starlette.requests import Request
from transformers import pipeline

ray.init()
serve.start(detached=True, http_options={"host": "0.0.0.0"})


@serve.deployment(num_replicas=1, ray_actor_options={"num_cpus": 0.2, "num_gpus": 0})
class EmbeddingModel:
    def __init__(self):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.pipeline = pipeline(
            task="feature-extraction",
            model="sentence-transformers/all-MiniLM-L6-v2",
            device=self.device,
        )

    def get_embedding(self, text: str) -> List[float]:
        # Run inference
        model_output = self.pipeline(text, return_tensors="pt")

        # Post-process output to return only the translation text
        mean_embedding = torch.mean(model_output, axis=1).cpu().tolist()
        return mean_embedding

    async def __call__(self, http_request: Request) -> List[float]:
        text: str = await http_request.json()
        output = self.get_embedding(text)
        return output


app = EmbeddingModel.bind()
