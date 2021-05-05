from DeltaChecker import DeltaChecker
from ScrapeTask import ScrapeTask

task = ScrapeTask(iterations=10, debug=False, on_proxy=True)
task.start()

deltaChecker = DeltaChecker()
deltaChecker.check_delta(deltaChecker.get_latest_timestamp())
