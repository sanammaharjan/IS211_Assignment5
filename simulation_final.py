import argparse, csv, urllib2, logging
from StringIO import StringIO

logging.basicConfig(dataname='error.log', level=logging.DEBUG)
parser = argparse.ArgumentParser(description='Assignment 5 to paas csv url from command line')
parser.add_argument('--url', action="store", dest="url", type=str)
parser.add_argument('--servers', type=int)
args = parser.parse_args()

def downloadData(url):
    '''
    This function read csv data from URL.
    :param url: Link for CSV data
    :return: Data for CSV
    '''
    request = urllib2.Request(url)
    csv_data = urllib2.urlopen(request)
    return csv_data.read()

class Queue:
    '''
    Defining queue
    '''

    def __init__(self):
        self.items = []

    def is_empty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0, item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)

class Server:
    '''
    Server Class
    '''

    def __init__(self):
        self.current_task = None
        self.time_remaining = 0

    def tick(self):
        if self.current_task != None:
            self.time_remaining = self.time_remaining - 1
            if self.time_remaining <= 0:
                self.current_task = None

    def busy(self):
        if self.current_task != None:
            return True
        else:
            return False

    def start_next(self, new_task):
        self.current_task = new_task
        self.time_remaining = new_task.get_length()


class Request:
    '''
    Request class
    '''

    def __init__(self, time, length):
        self.timestamp = time
        self.length = int(length)

    def get_stamp(self):
        return self.timestamp

    def get_length(self):
        return self.length

    def wait_time(self, cur_time):
        return cur_time - self.timestamp

def simulateOneServer(data):
    '''
    It simulate for one server
    :param data: It is source of data
    :return:
    '''
    server = Server()
    queue = Queue()
    waiting_times = []

    response = csv.reader(StringIO(data))

    for row in response:
        timestamp = int(row[0])
        length = int(row[2])
        req = Request(timestamp, length)
        queue.enqueue(req)

        if (not server.busy()) and (not queue.is_empty()):
            next_request = queue.dequeue()
            waiting_times.append(next_request.wait_time(timestamp))
            server.start_next(req)

        server.tick()

    average_wait = sum(waiting_times) / len(waiting_times)
    print 'Average Wait %6.2f secs %3d tasks remaining.' % (average_wait, queue.size())

def simulateManyServers(data, no_server):
    '''
    It simulate more than one server
    :param data: source of data
    :param no_server: no of server to simulate
    :return:
    '''
    servers = []
    for server_no in range(0, no_server):
        servers.append(Server())

    queues = []
    for server_no in range(0, no_server):
        queues.append(Queue())

    waiting_times = []
    for server_no in range(0, no_server):
        waiting_times.append([])

    response = csv.reader(StringIO(data))

    roundRobinPosition = 0

    for row in response:
        timestamp = int(row[0])
        length = int(row[2])
        req = Request(timestamp, length)

        queues[roundRobinPosition].enqueue(req)
        if roundRobinPosition < no_server - 1:
            roundRobinPosition += 1
        else:
            roundRobinPosition = 0

        if (not servers[roundRobinPosition].busy()) and (not queues[roundRobinPosition].is_empty()):
            next_request = queues[roundRobinPosition].dequeue()
            waiting_times[roundRobinPosition].append(next_request.wait_time(timestamp))
            servers[roundRobinPosition].start_next(req)

        servers[roundRobinPosition].tick()

    for server_no in range(0, no_server):
        average_wait = sum(waiting_times[server_no]) / len(waiting_times[server_no])
        print 'Average Wait %6.2f secs %3d tasks remaining.' % (average_wait, queues[server_no].size())

def main():
    if args.url:
        try:
            no_server = args.servers
            csvdata_url = downloadData(args.url)
            if no_server > 1:
                simulateManyServers(csvdata_url, no_server)
            else:
                simulateOneServer(csvdata_url)
        except Exception as e:
            logging.warning('Its a parameter error')
            print 'Something went wrong, please check the commands'

    else:
        print 'Please inpute URL from command line  '  # def main():
#     try:
#         mainurl = 'http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv'
#         server = 1
#         dataData = downloadData(mainurl)
#         if server > 1:
#             simulateManyServers(dataData, server)
#         else:
#             simulateOneServer(dataData)
#     except:
#         print 'Please check your data'

if __name__ == '__main__':
    main()