from typing import Iterable, Dict

from common.log import LogData
from common.statistics import median, percentile
from transformer.base import AverageTransformer


class SignatureAveragesTransformer(AverageTransformer):
    def __init__(self):
        super().__init__("signature", "signature-averages")

    def _average(self, log: LogData, start_time: float, end_time: float) -> Iterable[Dict]:
        data = [d for d in log.entries() if "operation" in d and start_time <= d["timestamp"] < end_time]
        sign = [d["duration"] for d in log.entries() if d["operation"] == "sign"]
        verify = [d["duration"] for d in log.entries() if d["operation"] == "verify"]

        med_sign = median(sign)
        lower_quart_sign = percentile(sign, 0.25)
        upper_quart_sign = percentile(sign, 0.75)

        med_verify = median(verify)
        lower_quart_verify = percentile(verify, 0.25)
        upper_quart_verify = percentile(verify, 0.75)

        return [
            {
                "timestamp": data[0]["timestamp"],
                "operation": "sign",
                "count": data[0]["count"],
                "median": round(med_sign, 3),
                "lower_quartile": lower_quart_sign,
                "upper_quartile": upper_quart_sign,
            },
            {
                "timestamp": data[0]["timestamp"],
                "operation": "verify",
                "count": data[0]["count"],
                "median": round(med_verify, 3),
                "lower_quartile": lower_quart_verify,
                "upper_quartile": upper_quart_verify,
            },
        ]
