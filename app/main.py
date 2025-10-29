import argparse
from app.classes import Master, Slave

def main(args):
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    if args.replicaof:
        runner = Slave(args)
    else:
        runner = Master(args)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replicaof", type=str, default="")
    parser.add_argument("--dir", type=str, default="")
    parser.add_argument("--dbfilename", type=str, default="")
    args = parser.parse_args()
    main(args)