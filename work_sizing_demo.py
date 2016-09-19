""" Demonstrates how sizing of worker blocks affects how long a multiprocess script takes to execute. """

from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor, as_completed,wait
from logging import basicConfig, getLogger, INFO, DEBUG
from math import ceil
from time import time

logger = getLogger(__name__)


def is_valid(n, upper_range_value):
    """ Check the number is evenly divisible for everything in the range - upper_ramge_value. """
    for i in range(1, upper_range_value):
        if not n % i == 0:
            return False
    return True


def find_in_range(start, end, upper_range_value):
    """ Determine if a number is evenly divisible for everything in the range start-emd.  Return it if found, None otherwise."""
    n = start
    while n < end:
        if is_valid(n, upper_range_value):
            return n
        n+=1
    return None


def find_multiple(upper_range_value, block_size, max_workers, max_jobs, timeout_in_seconds):
    """ Finf the smallest multiple that must be evenly divisible with the numbers 1-upper_range_value. """
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        start = 1
        futures = {}
        sum_of_available_workers = 0
        total_loops = 1
        logger.debug("max workers: %d, max jobs: %d", max_workers, max_jobs)
        while True:
            block_start = start
            futures_length = len(futures)
            available_workers = max_workers-futures_length if max_workers > futures_length else 0
            sum_of_available_workers += available_workers

            for i in range(futures_length, max_jobs):
                futures[executor.submit(find_in_range, start, start + block_size, upper_range_value=upper_range_value)] = (start, start + block_size)
                start += block_size

            done, not_done = wait(futures, timeout=timeout_in_seconds)
            for future in done:
                del futures[future]
                if future.result() is not None:
                    executor.shutdown()
                    logger.debug("Average number of workers available (optimally 0): %f", sum_of_available_workers / total_loops)
                    return future.result()
            total_loops += 1


def get_args():
    parser = ArgumentParser(description='Lowest multiple/evenly divisible finder and work task distribution example.')
    parser.add_argument('n', type=int, help='1 to n will be the range the multiple must be evenly divisible by.')
    parser.add_argument('-b', '--block_size', type=int, help='Size of a block distributed to a process. Default: 100', default=100)
    parser.add_argument('-w', '--max_workers', type=int, help='Number of processes to spawn. Default: 20', default=20)
    parser.add_argument('-j', '--max_jobs', type=int, help='Max jops to submit to workers. Default=max_workers', default=0)
    parser.add_argument('-t', '--timeout', type=float, help='Number of seconds to wait before checking status. Default: 0.1', default=0.1)
    parser.add_argument('-v', '--verbose', help='Verbose log output', default=False, action='store_true')
    return parser.parse_args()


if __name__ == "__main__":

    args = get_args()
    if args.max_jobs == 0:
        args.max_jobs = args.max_workers

    logging_config = dict(level=DEBUG if args.verbose else INFO, format='[%(asctime)s - %(filename)s:%(lineno)d - %(funcName)s - %(levelname)s] %(message)s')

    basicConfig(**logging_config)

    logger.debug("Starting up.\n\nSettings:\nMust be evenly divisible 1-%d\nblock_size=%d\nmax_workers=%d\ntimeout=%f\n", args.n, args.block_size, args.max_workers, args.timeout)

    t1 = time()
    result = find_multiple(args.n, args.block_size, args.max_workers, args.max_jobs, args.timeout)
    t2 = time()

    print("Smallest Multiple Found: {0} in {1:.3}s".format(result, t2-t1))



