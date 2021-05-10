from ScrapeTask import ScrapeTask

task = ScrapeTask(iterations=-1, interval_seconds=60*5, debug=False, on_proxy=False)
task.start()

