"""
Norgate config file validator. This cross-checks the ng_config_futures.csv with our instrument config database

"""
from sysdata.norgate.ng_database import norgateInstrumentDatabase, NG_CONFIG_FILE, NG_ID_COLUMN, ID_COLUMN
from sysdata.mongodb.mongo_futures_instruments import mongoFuturesInstrumentData
from sysbrokers.IB.ib_instruments_data import ibFuturesInstrumentData
from syscore.objects import missing_instrument

exchange_groups = [
                    ["CME","GLOBEX","CMECRYPTO"],
                    ["EUREX","Eurex","SOFFEX","DTB"],
                    ["MONEP" , "EURONEXT"],
                    ["ECBOT","CBOT","KCBT"],
                    ["KSE","KRX"],
                    ["SGX","SGX-DT"],
                    ["CFE","CBOE"],
                    ["SNFE","SFE"],
                    ["NYBOT", "ICE US"],
                    ["ENDEX", "ICE"],
                    ["ICEEU","ICEEUSOFT","ICE"],
]

mongo_data = mongoFuturesInstrumentData()
ib_data = ibFuturesInstrumentData(ibconnection=None)

class norgateValidator(object):

    def __init__(self):

        self.db = norgateInstrumentDatabase()

    def validate_config(self):
        print("Checking", NG_CONFIG_FILE, "integrity..")

        ids = self.db.config

        # drop nans
        our_ids = ids[ids[ID_COLUMN]==ids[ID_COLUMN]]
        ng_ids = ids[ids[NG_ID_COLUMN]==ids[NG_ID_COLUMN]]


        # duplicate ids?
        df = our_ids[our_ids.duplicated([ID_COLUMN])]
        if len(df) > 0:
            print("Error! Found duplicate Ids: ", df[ID_COLUMN].to_list())

        # duplicate Ng ids?
        df = ng_ids[ng_ids.duplicated([NG_ID_COLUMN])]
        if len(df) > 0:
            print("Error! Found duplicate Norgate Ids: ", df[NG_ID_COLUMN].to_list())

        # every id present in config file?
        instruments = mongo_data.get_list_of_instruments()
        for instr in instruments:
            if len(our_ids[our_ids[ID_COLUMN]==instr])==0:
                print("Notice: pysystemtrade instrument ",instr,"not found in Norgate config file")

        # every id found in config file is also registered in pysystemtrade?
        for cfg_id in our_ids[ID_COLUMN].to_list():
            if cfg_id not in instruments:
                print("Error! Unknown Id found in config file: ", cfg_id)


    def crosscheck_instrument(self, instrument:str ):

        our_instr = mongo_data.get_instrument_data(instrument).meta_data
        db = self.db
        valid = True

        our_ib_instr = ib_data.get_futures_instrument_object_with_IB_data(instrument)
        if our_ib_instr is missing_instrument:
            print("Error! Instrument", instrument," is missing from IB config file!")
            return False
        our_ib_instr = our_ib_instr.ib_data

        ng_instr = db.get_ng_instrument_metadata(instrument)
        if ng_instr is missing_instrument:
            print(" - Warning!",instrument," does not have Norgate instrument database entry!")
            return False


        if our_instr.Currency != ng_instr.Currency:
            print(" - Error!", instrument, " currency mismatch! ",our_instr.Currency,  "!=",ng_instr.Currency)
            valid = False

        # check if we have matching exchanges
        exch_match = False
        if our_ib_instr.exchange == ng_instr.Exchange:
            exch_match = True
        for exch_group in exchange_groups:
            if ng_instr.Exchange in exch_group and our_ib_instr.exchange in exch_group:
                exch_match = True
        if exch_match == False:
            print(" - Error!", instrument, " exchange mismatch! ",our_ib_instr.exchange,  "!=",ng_instr.Exchange)
            valid = False

        # ***  Point size checks ****

        # IB has variable multipliers for contracts in case of few rare instruments 
        # (e.g. SILVER (SI) has 1000 and 5000) and the contract we are trading has 
        # different (usually the smaller) multiplier from what other market data providers use to calculate 
        # point size. 
        # To account for these we use myMultiplier in ib_config_futures.csv to silence any warnings
        pointsize = our_instr.Pointsize
        pointsize_mult = our_ib_instr.myMultiplier
        if pointsize_mult != 1:
            print(" - Note! For instrument", instrument, "using different multiplier ",pointsize*pointsize_mult, "to compare point size to NG database. This is ok")

        # Sometimes the price data fetched from IB has full currency units even though in the contract
        # specs indicate that price quotations are in fact different, for example "cents/lb"! In these rare
        # cases we have to multiply the point size and contract prices imported from sources other than IB!
        #
        # Example: Instr COPPER. Contract size: 25000 lbs. Units: cents/lb -> Point value = 250 USD. However
        # IB prices are reported as full dollars and IB multiplier is 25000. Thus we set the multiplier
        # to 0.01 so imported prices (cents/lb) are converted to USD/lb which is the same unit as 
        # used by IB
        unit_mult = db.get_unit_multiplier(instrument)
        if unit_mult != 1:
            print(" - Note! For instrument", instrument, "using unit multiplier ",unit_mult)
        if pointsize * pointsize_mult * unit_mult != ng_instr.PointValue:
            print(" - Error!", instrument, " point size mismatch! ",
                pointsize, "*", pointsize_mult ,"*", unit_mult," !=  ", ng_instr.PointValue)
            valid = False
        
        return valid


if __name__ == "__main__":

    print("Cross-check pysystemtrade instrument metadata with Norgate config file")
    instruments = mongo_data.get_list_of_instruments() 
    print("Pysystemtrade instruments: ", instruments)
    
    ngv = norgateValidator()
    ngv.validate_config()

    for instr in instruments:
        ng_id = ngv.db.get_ng_id(instr)
        if ng_id is not missing_instrument:
            print("Checking instrument ", instr)
            res = ngv.crosscheck_instrument(instr)
