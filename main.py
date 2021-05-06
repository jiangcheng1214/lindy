from DeltaChecker import DeltaChecker
from ScrapeTask import ScrapeTask

task = ScrapeTask(iterations=5, debug=True, on_proxy=True)
task.start()

# deltaChecker = DeltaChecker()
# deltaChecker.check_delta(deltaChecker.get_latest_timestamp())
