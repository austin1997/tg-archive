import asyncio
import random

async def worker(name, queue):
    while True:
        # Get a "work item" out of the queue.
        priority, item = await queue.get()
        print(f'{name} processing item {item} with priority {priority}')
        # Simulate I/O operation
        await asyncio.sleep(random.random())
        # Notify the queue that the "work item" has been processed.
        queue.task_done()
        print(f'{name} finished processing item {item}')

async def main():
    # Create a priority queue.
    queue = asyncio.PriorityQueue()

    # Create three worker tasks to process the queue concurrently.
    tasks = []
    for i in range(3):
        task = asyncio.create_task(worker(f'worker-{i}', queue))
        tasks.append(task)

    # Put items into the queue with random priorities.
    for i in range(10):
        priority = random.randint(1, 10)
        await queue.put((priority, f'item-{i}'))
        print(f'main put item-{i} with priority {priority}')

    # Wait until the queue is fully processed.
    await queue.join()

    # Cancel the worker tasks.
    for task in tasks:
        task.cancel()
    # Wait until all worker tasks are cancelled.
    await asyncio.gather(*tasks, return_exceptions=True)

    print('====')
    print(f'Final queue size: {queue.qsize()}')

if __name__ == '__main__':
    asyncio.run(main())
