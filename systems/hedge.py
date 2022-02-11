from systems.stage import SystemStage
from systems.system_cache import diagnostic
import pandas as pd

class Hedge(SystemStage):
    @property
    def name(self):
        return "hedge"

    @diagnostic()
    def get_buffered_position(
        self, instrument_code: str, roundpositions: bool = True
    ) -> pd.Series:
        raise NotImplementedError("Need to be implemented by child class")
