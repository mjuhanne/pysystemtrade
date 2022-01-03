"""
CSI instrument database validator. Reads instrument data from several sources (instrument config, IB instrument config) 
and its own csi-config.csv which contain mapping of CSI iD <-> instument code as well as possible price unit conversion weights.

Then it does following checks for instruments found in our database and the related instrument in CSI database:
    - Does currency match?
    - Does exchange match?
    - Are there any duplicate instrument codes or CSI ids in config file?
    - Do all instrument codes found in config file exist in our system?
    - Are all instrument codes found in config file?
    - Are all CSI ids in config file found in the respective CSI exchange config files?
    - Does the contracts full point size match the multiplier configured in instrumentconfig.csv?   
    - For those instruments having roll parameter config, check if it's consistent with the data CSI has for active contract months
"""

from sysdata.mongodb.mongo_futures_instruments import mongoFuturesInstrumentData
from sysdata.mongodb.mongo_roll_data import mongoRollParametersData
from sysbrokers.IB.ib_instruments_data import ibFuturesInstrumentData
from syscore.dateutils import month_from_contract_letter
from sysdata.csi.csi_database import csiInstrumentDatabase, CSI_CONFIG_FILE,CSI_ID_COLUMN, ID_COLUMN
import pandas as pd

from syscore.objects import missing_instrument

# Equivalencies between exchange reported by IB and CSI. Though they may not technically all be the same
# exchange (i.e. NYMEX/GLOBEX != CLEAR,) but some contracts are listed on both exchanges and
# CSI has them listed only on the other one (e.g. BRENT, BRENT-LAST, COTTON and PIPELINE on ClearPort)
exchange_groups = [
                    ["CME","GLOBEX","CMECRYPTO"],
                    ["NYMEX","COMEX","CLEAR"],
                    ["EUREX","DTB","SOFFEX"],
                    ["KSE","KRX"],
                    ["ECBOT","CBT"],
                    ["FTA" , "EURONEXT-AMSTERDAM", "EOE"],
                    ["MATIF" , "EURONEXT"],
                    ["MONEP" , "EURONEXT-PARIS", "EURONEXT"],
                    ["BELFOX","EURONEXT-BRUSSELS", "EURONEXT"],
                    ["OSE.JPN" , "OSE"],
                    ["SNFE" , "ASX"],
                    ["ICEEU" , "ICE-EU", "ICEEUSOFT", "ICE-EU-AGS","ICE-EU-FIN", "IPE","ENDEX"],
                    ["ICE-US", "NYBOT"] 
    ]

# CSI data implies that some of the instruments are only sometimes traded
# but this isn't true so suppress warnings about these 
hold_roll_cycle_exceptions = ["AEX","CAC","VIX","FTSECHINAA","FTSETAIWAN","MSCISING","NIFTY"]

OUR_DESC_COL = "OurDescription"
CSI_DESC_COL = "CSI_Description"

# Absolute minimum liquidity (just for the start to omit basically non active contracts. We can later have more strict requirements )
MINIMUM_VOLUME = 10 

ignore_missing_csi_db_entry = True
suppress_missing_roll_config_entry_warning = True
skip_illiquid_instruments = True

mongo_data = mongoFuturesInstrumentData()
ib_data = ibFuturesInstrumentData(ibconnection=None)
rollparameters = mongoRollParametersData()


class csiValidator(object):

    def __init__(self):
        self.db = csiInstrumentDatabase()


    def validate_id_config(self):
        print("Checking", CSI_CONFIG_FILE, "integrity..")

        # duplicate ids?
        ids = self.db.config
        df = ids[ids.duplicated([ID_COLUMN])]
        if len(df) > 0:
            print("Error! Found duplicate Ids: ", df[ID_COLUMN].to_list())

        # duplicate CSI ids?
        # drop nans
        df = ids[ids[CSI_ID_COLUMN]==ids[CSI_ID_COLUMN]]
        df = df[df.duplicated([CSI_ID_COLUMN])]
        if len(df) > 0:
            print("Error! Found duplicate CSI Ids: ", df[CSI_ID_COLUMN].to_list())

        # every id present in config file?
        instruments = mongo_data.get_list_of_instruments()
        for instr in instruments:
            if len(ids[ids[ID_COLUMN]==instr])==0:
                print("Error! Instrument ",instr,"not found in config file!")

        # every id found in config file is also registered in pysystemtrade?
        for cfg_id in ids[ID_COLUMN].to_list():
            if cfg_id not in instruments:
                print("Error! Unknown Id found in config file: ", cfg_id)


    def crosscheck_instrument(self, instrument:str ):
        our_instr = mongo_data.get_instrument_data(instrument).meta_data
        db = self.db

        our_ib_instr = ib_data.get_futures_instrument_object_with_IB_data(instrument)
        if our_ib_instr is missing_instrument:
            print("Error! Instrument", instrument," is missing from IB config file!")
            return False
        our_ib_instr = our_ib_instr.ib_data

        csi_id = db.get_csi_id(instrument)
        if csi_id is missing_instrument:
            if ignore_missing_csi_db_entry == False:
                print(" - Warning!",instrument," does not have CSI database entry!")
            return False

        csi_instr = db.get_csi_instrument_metadata(csi_id)
        if csi_instr is missing_instrument:
            print(" - Error! Instrument %s (%d) not found in CSI database!" % (instrument, csi_id))
            return False

        print("Processing instrument %s (%s)" % (instrument, csi_id))

        valid = True

        if our_instr.Currency != csi_instr.Currency:
            if our_instr.Currency == "CNH" and csi_instr.Currency == "CNY":
                # for the off-shore Yuan (CNH) instruments the CSI market data states erroneusly 
                # that the currency is CNY (on-shore Yuan). We'll let it pass..
                pass
            else:
                print(" - Error!", instrument, " currency mismatch! ",our_instr.Currency,  "!=",csi_instr.Currency)
                valid = False

        # check if we have matching exchanges
        exch_match = False
        if our_ib_instr.exchange == csi_instr.Exchange:
            exch_match = True
        for exch_group in exchange_groups:
            if csi_instr.Exchange in exch_group and our_ib_instr.exchange in exch_group:
                exch_match = True
        if exch_match == False:
            print(" - Error!", instrument, " exchange mismatch! ",our_ib_instr.exchange,  "!=",csi_instr.Exchange)
            valid = False

        # does point size checks

        if our_instr.Pointsize != our_ib_instr.ibMultiplier and our_ib_instr.ibMultiplier != '':
            print(" - Notice! pysystemtrade Pointsize %d != IBmultiplier %d (Irregular unit but most likely ok. Check before trading!)" % (our_instr.Pointsize, our_ib_instr.ibMultiplier))


        # IB has variable multipliers for contracts in case of few rare instruments 
        # (e.g. SILVER (SI) has 1000 and 5000) and we have selected as our multiplier
        # different (usually the smaller) from what other market data providers use to calculate 
        # point size. 
        # To account for these we use myMultiplier in ib_config_futures.csv to silence any warnings
        pointsize = our_instr.Pointsize
        pointsize_mult = our_ib_instr.myMultiplier
        if pointsize_mult != 1:
            print(" - Notice! For instrument", instrument, "using different contract multiplier ",pointsize*pointsize_mult, "to compare point size to CSI database. This is ok")

        # Sometimes the price data fetched from IB has full currency units (e.g. USD/lb) even though the contract
        # specs (on exchange www page) indicate that price quotations are in fact different (for example cents/lb)! In these rare
        # cases we have to multiply the point size and contract prices imported from sources other than IB!
        #
        # Example: Instr COPPER. Contract size: 25000 lbs. Units: cents/lb -> Point value = 250 USD. However
        # IB prices are reported as full dollars and IB multiplier is 25000. Thus we set the multiplier
        # to 0.01 so imported prices (cents/lb) are converted to USD/lb which is the same unit as 
        # used by IB
        unit_mult = float(db.get_unit_multiplier(instrument))
        if unit_mult != 1:
            print(" - Note! For instrument", instrument, "using unit multiplier ",unit_mult)
        if pointsize * pointsize_mult * unit_mult != csi_instr.FullPointValue:
            print(" - Error!", instrument, " point size mismatch! ",
                pointsize, "*", pointsize_mult ,"*", unit_mult," !=  ", csi_instr.FullPointValue)
            print("   Check myMultiplier and CSI database Unit of Measurement!")
            valid = False

        if csi_instr.IsActive != 1:
            print(" - Warning!", instrument, "not active!")
            valid = False

        if csi_instr.LastTotalVolume < MINIMUM_VOLUME:
            if skip_illiquid_instruments == False:
                print(" - Warning!", instrument, " is illiquid!")
                valid = False


        # the following checks are for additional peace of mind if you want to trade the instrument but failing them doesn't
        # prevent us from writing CSI portfolio entry since we might want to use it for analyzing purposes

        try:
            roll_param = rollparameters.get_roll_parameters(instrument)
        except:
            if suppress_missing_roll_config_entry_warning == False:
                print(" - Error! Roll parameters for instrument", instrument," not found !!")
            return valid

        # is every holding contract valid?
        hold = roll_param.hold_rollcycle.cyclestring
        for i in range(len(hold)):
            m = month_from_contract_letter(hold[i])
            if csi_instr.DeliveryMonths[m-1] == 'I':
                print(" - Error! Instrument", instrument, "hold roll cycle has invalid month (",
                    hold[i],")! (hold:", hold, " vs csiData:", csi_instr.DeliveryMonths,")")
            else:
                if csi_instr.DeliveryMonths[m-1] == 'S' and instrument not in hold_roll_cycle_exceptions:
                    print(" - Warning! Instrument", instrument, "hold roll cycle month ",hold[i],
                        " only sometimes traded! (hold:", hold, " vs csiData:", csi_instr.DeliveryMonths,")")

        # is every priced contract valid?
        priced = roll_param.priced_rollcycle.cyclestring
        for i in range(len(priced)):
            m = month_from_contract_letter(priced[i])
            if csi_instr.DeliveryMonths[m-1] == 'I':
                print(" - Error! Instrument", instrument, "priced roll cycle has invalid month (",
                    priced[i],")! (priced:", priced, " vs csiData:", csi_instr.DeliveryMonths,")")
        
        return valid


if __name__ == "__main__":

    print("Cross-check pysystemtrade instrument metadata with CSI database")
    instruments = mongo_data.get_list_of_instruments() 

    dbv = csiValidator()
    for instr in instruments:
        res = dbv.crosscheck_instrument(instr)
