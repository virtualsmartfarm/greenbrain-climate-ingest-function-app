import logging
import azure.functions as func

def main(inputBlob: func.InputStream, outputBlob: func.Out[str]):
    logging.info(f"inputBlob.name: {inputBlob.name}\n")
    logging.info(f"inputBlob.length: {inputBlob.length}\n")
    logging.info(f"inputBlob.uri: {inputBlob.uri}\n")

    if {inputBlob.name} == 'greenbrain/curated/greenbrain-59213airtempmin-degree.csv':
        logging.info('Matched \'curated/greenbrain-59213airtempmin-degree.csv')
        # outputBlob.set(inputBlob.read())
    else:
        logging.info('Didn\'t match \'curated/greenbrain-59213airtempmin-degree.csv\'')

    if {inputBlob.name} == 'greenbrain/curated/greenbrain-59214airtempavg-degree.csv':
        logging.info('Matched \'curated/greenbrain-59214airtempavg-degree.csv')
        # outputBlob.set(inputBlob.read())
    else:
        logging.info('Didn\'t match \'curated/greenbrain-59214airtempavg-degree.csv\'')

    if {inputBlob.name} == 'greenbrain/curated/greenbrain-59215airtempmax-degree.csv':
        logging.info('Matched \'curated/greenbrain-59215airtempmax-degree.csv')
        # outputBlob.set(inputBlob.read())
    else:
        logging.info('Didn\'t match \'curated/greenbrain-59215airtempmax-degree.csv\'')