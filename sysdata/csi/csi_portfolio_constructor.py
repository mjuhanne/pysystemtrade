"""
CSI data portfolio constructor. This script will read all the CSI futures instruments from csi-config.csv 
and write a csv file containing CSI ids, which you can then import to Unfair Advantage as a new portfolio:
(Portfolio -> Import Custom Porfolio -> ASCII Import.  Field 1 must be set to 'CSI #' and Field 2 to 'ignore')

The instrument inclusion criteria is: 
    - 'Enabled' field is '1'
    - minimum daily contract volume > MINIMUM_AVG_YEARLY_VOLUME 
    - CSI instrument passes validator cross checking. 

You can create your custom porfolio by changing these settings (modify "Enabled" setting in csi-config.csv),
change MINIMUM_AVG_YEARLY_VOLUME and modify force_include/exclude_instruments lists below

"""
from sysdata.mongodb.mongo_futures_instruments import mongoFuturesInstrumentData
from sysdata.csi.csi_database_validator import csiValidator
from sysdata.csi.csi_database import CSI_ID_COLUMN, ID_COLUMN, AVG_VOLUME_COL
import pandas as pd
from syscore.objects import missing_instrument

OUTPUT_FILE = "portfolio.csv"
MINIMUM_AVG_YEARLY_VOLUME = 1000
ignore_illiquid_instruments = True

mongo_data = mongoFuturesInstrumentData()

# we might want to include these even though average yearly volumes do not cross the threshold
force_include_instruments = ['ALUMINIUM','SEK','NOK','KRWUSD_mini','LUMBER','CHEESE',
    'COPPER-mini','WHEAT_mini','BUTTER','HIGHYIELD','STEEL','RICE','OATIES']

# let's exclude these arbitrarily chosen instruments because they are highly correlated with other instruments
# and then there is the 150 max instrument limit on CSI's Unfair Advantage..
force_exclude_instruments = ['EUROSTX-MID','EURO600','REDWHEAT','R1000','SP400']


def write_portfolio_csv(filename):

    dbv = csiValidator()
    db = dbv.db
    df = pd.DataFrame(columns=[CSI_ID_COLUMN, ID_COLUMN])

    instruments = mongo_data.get_list_of_instruments()

    instr_count = 0
    for instr in instruments:

        enabled = db.is_enabled(instr)  # returns 0 also when instrument is not found in CSI database
        
        if enabled == 1:

            csi_id = db.get_csi_id(instr)

            assert(csi_id != missing_instrument)

            csi_instr = db.get_csi_instrument_metadata(csi_id)
            assert(csi_instr is not missing_instrument)

            if csi_instr is not missing_instrument:

                valid = dbv.crosscheck_instrument(instr)
                if valid == True:

                    our_instr = mongo_data.get_instrument_data(instr).meta_data

                    avg_yearly_vol = csi_instr[AVG_VOLUME_COL]
                    if avg_yearly_vol < MINIMUM_AVG_YEARLY_VOLUME and ignore_illiquid_instruments==True:
                        if instr in force_include_instruments:
                            print("  >>>>> Force adding instrument %s even though illiquid (avg yearly volume %d, last total volume %d)" % (instr, avg_yearly_vol, csi_instr.LastTotalVolume))
                        else:
                            print("  ----- Skipping illiquid %s (avg yearly volume %d, last total volume %d)" % (instr, avg_yearly_vol, csi_instr.LastTotalVolume))
                            continue

                    if instr in force_exclude_instruments:
                        print("  ===== Force exclude instrument %s" % instr)
                        continue

                    csi_id = int (csi_id)
                    print("  +++++ Adding instrument %s" % instr )
                    row = { 
                            ID_COLUMN : instr, 
                            CSI_ID_COLUMN : csi_id,
                        }
                    df = df.append(row, ignore_index=True)
                    instr_count += 1
        else:
            print("Skipping disabled/missing instrument", instr)

    print("Saving", instr_count, "instruments...")
    f = open(filename,"w")
    df.to_csv(f, index=False, header=False)
    f.close()


if __name__ == "__main__":
    print("CSI data portfolio constructor. Saving to %s" % OUTPUT_FILE)
    write_portfolio_csv(OUTPUT_FILE)
