from typing import List, Tuple

import ray
import torch
from ray import serve
from starlette.requests import Request
from transformers import AutoModelForSequenceClassification, AutoTokenizer

ray.init()
serve.start(detached=True, http_options={"host": "0.0.0.0"})


@serve.deployment(num_replicas=1, ray_actor_options={"num_cpus": 0.2, "num_gpus": 0})
class RankingModel:
    def __init__(self):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = AutoModelForSequenceClassification.from_pretrained(
            "cross-encoder/ms-marco-MiniLM-L12-v2",
        ).to(self.device)
        self.tokenizer = AutoTokenizer.from_pretrained(
            "cross-encoder/ms-marco-MiniLM-L12-v2"
        )
        self.model.eval()

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
        pairs: List[Tuple[str, str]] = await http_request.json()
        return str(self.get_ranking(pairs))


app = RankingModel.bind()
