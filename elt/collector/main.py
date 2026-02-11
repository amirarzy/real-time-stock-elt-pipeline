from elt.collector.collectors.yahoo import YahooRealtimeCollector
from elt.collector.utils.logger import get_logger


def main():
    logger = get_logger()
    logger.info("Starting market data collector...")

    collector = YahooRealtimeCollector()
    collector.run()


if __name__ == "__main__":
    main()
