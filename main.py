from DeltaChecker import DeltaChecker
from ScrapeTask import ScrapeTask

task = ScrapeTask(iterations=5, debug=False, on_proxy=False)
task.start()

# deltaChecker = DeltaChecker()
# deltaChecker.check_delta(deltaChecker.get_latest_timestamp())
