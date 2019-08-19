import logging
import threading
import time

# class FakeDatabase:
#     def __init__(self):
#         self.value = 0

#     def update(self, name):
#         logging.info("Thread %s: starting update", name)
#         local_copy = self.value
#         local_copy += 1
#         time.sleep(0.001)
#         self.value = local_copy
#         logging.info("Thread %s: finishing update", name)

class FakeDatabase:
    def __init__(self):
        self.value = 0
        self.lock = threading.Lock()

    def update(self, name):
        logging.info("Thread %s: starting update", name)
        logging.debug("Thread %s about to lock", name)
        with self.lock:
            logging.debug("Thread %s has lock", name)
            local_copy = self.value
            local_copy += 1
            time.sleep(0.1)
            self.value = local_copy
            logging.debug("Thread %s about to release lock", name)
        logging.debug("Thread %s after release", name)
        logging.info("Thread %s: finishing update", name)

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO,
                    datefmt="%H:%M:%S")

database = FakeDatabase()

logging.info("Testing update. Starting value is %d.", database.value)


# database.update(1)
# database.update(2)

threads = []
for index in range(50):
    # database.update(index)
    thread = threading.Thread(target=database.update, args=(index,))
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

logging.info("Testing update. Ending value is %d.", database.value)