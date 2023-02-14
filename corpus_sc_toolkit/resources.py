from pathlib import Path

SC_BASE_URL = "https://sc.judiciary.gov.ph"
DECISION_PATH: Path = Path().home().joinpath("code/corpus/decisions")
SC_LOCAL_FOLDER: Path = DECISION_PATH / "sc"
