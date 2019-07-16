
import pymongo
import argparse
import sys
import pprint

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--host", default="mongodb://localhost:27017")
    parser.add_argument("--db", default="test")
    parser.add_argument("--source", default="source")
    parser.add_argument("--sink", default=None)
    parser.add_argument("--limit", default=0, type=int)
    parser.add_argument("--fieldname", default="price")
    parser.add_argument("--converter", default="$toInt",
                        choices=["$toInt", "$toBool", "$toDate",
                                 "$toDecimal", "$toDouble", "$toInt",
                                 "$toLong", "$toObjectId", "$toString"])

    args = parser.parse_args()

    client = pymongo.MongoClient(host=args.host)
    db=client[args.db]
    collection=db[args.source]

    if args.source == args.sink:
        print(f"{args.fieldname} source and sink can't be the same collection")
        sys.exit(1)

    aggregator = []
    if args.limit:
        limiter = {"$limit": args.limit}
        aggregator.append(limiter)
    matcher = {"$match" : {args.fieldname:{"$exists": 1 }}}
    adder = {"$addFields" : {args.fieldname: { args.converter: "$"+args.fieldname}}}

    aggregator.append(matcher)
    aggregator.append(adder)

    if args.sink:
        aggregator.append( {"$out": args.sink})

    print("Aggregator")
    print(f"{aggregator}")
    cursor = collection.aggregate(aggregator)

    if not args.sink:
        for i in cursor:
            pprint.pprint(i)
