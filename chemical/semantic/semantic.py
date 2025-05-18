from typing import List, Tuple

import torch
from ray import serve
from starlette.requests import Request
from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline


@serve.deployment(num_replicas=1, ray_actor_options={"num_cpus": 0.2, "num_gpus": 0})
class SemanticModel:
    def __init__(self):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.pipeline = pipeline(
            task="feature-extraction",
            model="sentence-transformers/all-MiniLM-L6-v2",
            device=self.device,
        )
        self.model = AutoModelForSequenceClassification.from_pretrained(
            "cross-encoder/ms-marco-MiniLM-L12-v2",
        ).to(self.device)
        self.tokenizer = AutoTokenizer.from_pretrained(
            "cross-encoder/ms-marco-MiniLM-L12-v2"
        )
        self.model.eval()

    def get_embedding(self, text: str) -> List[float]:
        # Run inference
        model_output = self.pipeline(text, return_tensors="pt")

        # Post-process output to return only the translation text
        mean_embedding = torch.mean(model_output, axis=1).cpu().tolist()
        return mean_embedding

    def get_ranking(self, pairs: List[Tuple[str, str]]) -> List[float]:
        features = self.tokenizer(
            pairs,
            padding=True,
            truncation=True,
            return_tensors="pt",
        )

        with torch.no_grad():
            scores = (
                self.model(**features, return_dict=True)
                .logits.view(
                    -1,
                )
                .float()
            )
            scores = torch.sigmoid(scores).cpu().tolist()

        return scores

    async def __call__(self, http_request: Request) -> List[float]:
        request = await http_request.json()

        text, name = request["text"], request["name"]

        if name == "embedding":
            output = self.get_embedding(text)
        elif name == "ranking":
            output = self.get_ranking(text)
        else:
            raise ValueError(f"Invalid name: {name}")

        return output


semantic_app = SemanticModel.bind()
