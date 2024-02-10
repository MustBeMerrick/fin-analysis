import argparse
import csv
import pathlib
import pandas as pd
from collections import deque
import locale
locale.setlocale( locale.LC_ALL, '' )

# global FIFO
date_deque = deque("")
quant_deque = deque()
notional_deque = deque()

# FIFO push
def fifo_push(date, quantity, amount):
  date_deque.append(date)
  quant_deque.append(quantity)
  notional_deque.append(amount)

# FIFO pop
def fifo_pop():
  date = date_deque.popleft()
  quantity = quant_deque.popleft()
  amount = notional_deque.popleft()

  return date, quantity, amount

# FIFO modify tail
def fifo_modify_tail(quantity, amount):
  quant_deque[0]    -= quantity
  notional_deque[0] -= amount

#def convert_nmbrs_2_csv():
  # To Do

# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("-u", "--underlier", help="Name of underlier (stock ticker)")
parser.add_argument("-d", "--debug", action='store_true', help="print debug info. I.e. src line items")
args = parser.parse_args()

# read input file (all transactions)
#convert_nmbrs_2_csv()
# get path to src RH data
csv_file = str(pathlib.Path(__file__).parent.resolve()) + "/../data/robinhood/rh_transactions.csv"
df = pd.read_csv (csv_file)

# build df for desired underlier
df_und=df[df['Instrument'] == args.underlier]
df_und = df_und.reset_index()
df_und = df_und.drop(columns=['index'])
df_und['Amount'] = df_und['Amount'].replace(to_replace="\((\$.+\.[0-9][0-9])\)", value=r'-\1', regex=True)

# fetch all buys/sells/splits/ACATS for desired underlier
df_und_bs = df_und[(df_und['Trans Code'] ==  "Buy") | 
                   (df_und['Trans Code'] == "Sell") | 
                   (df_und['Trans Code'] ==  "SPL") |
                   (df_und['Trans Code'] == "ACATS")]
df_und_bs = df_und_bs.reset_index()
df_und_bs = df_und_bs.drop(columns=['index'])
df_und_bs['Amount'] = df_und_bs['Amount'].replace(to_replace=[',', '\$'], value=['', ''], regex=True)

if args.debug:
  print(df_und_bs)
  print()

# iterate over all transactions
for idx in reversed(df_und_bs.index):
  if (df_und_bs.loc[idx]['Trans Code'] == "Buy"):
    # buy = push to FIFO
    fifo_push(df_und_bs.loc[idx]['Activity Date'], float(df_und_bs.loc[idx]['Quantity']), float(df_und_bs.loc[idx]['Amount']))
  elif (df_und_bs.loc[idx]['Trans Code'] == "Sell"):
    # sell = pop from FIFO
    print_str = "Sold " + df_und_bs.loc[idx]['Quantity'] + " shares on " + df_und_bs.loc[idx]['Activity Date'] + " for " + df_und_bs.loc[idx]['Price'] + "/share " + "(Notional: " + locale.currency(float(df_und_bs.loc[idx]['Amount']), grouping=True) + ")"
    print("-------------------------------", print_str, "-------------------------------")
    print("Security    Quantity   Date Acquired       Price      Cost Basis          PL".format())
    remaining_quantity = float(df_und_bs.loc[idx]['Quantity'])
    proceeds_per_share = float(df_und_bs.loc[idx]['Amount'])/remaining_quantity
    pl_aggr = 0
    while remaining_quantity > 0:
      if (remaining_quantity >= quant_deque[0]):
        # entire bucket will be popped

        # grab date/quantity/amount from the oldest purchase bucket
        buy_date, buy_quantity, buy_notional = fifo_pop()

        # calculate cost basis per share of the bucket
        cb_per_share = buy_notional/buy_quantity

        # accumulate P/L and update remaining_quantity
        pl_i = (proceeds_per_share + cb_per_share) * buy_quantity
        pl_aggr += pl_i
        remaining_quantity -= buy_quantity
      else:
        # Tail of FIFO will be modified

        # not entire quantity will be ammortized- just remaining quantity
        buy_quantity = remaining_quantity
        buy_date = date_deque[0]

        # calculate cost basis per share of the bucket in use
        cb_per_share = notional_deque[0]/quant_deque[0]

        # update FIFO tails
        fifo_modify_tail(buy_quantity, cb_per_share * buy_quantity)

        # accumulate P/L and update remaining_quantity
        pl_i = (proceeds_per_share + cb_per_share) * buy_quantity
        pl_aggr += pl_i
        remaining_quantity -= buy_quantity

      # print purchase sub-line
      acq_print_str = "{:>8}{:>12}{:>16}{:>12}{:>16}{:>12}".format(args.underlier, "{:.5f}".format(buy_quantity), buy_date, locale.currency(abs(cb_per_share)), locale.currency(abs(cb_per_share*buy_quantity), grouping=True), locale.currency(pl_i, grouping=True))
      print(acq_print_str)

    print("P/L:", locale.currency(pl_aggr, grouping=True))

  elif (df_und_bs.loc[idx]['Trans Code'] == "SPL"):
    # handle stock split

    cum_shares = 0
    add_shares = float(df_und_bs.loc[idx]['Quantity'])
    
    # calculate total shares owned
    for d_i in range(len(quant_deque)):
      cum_shares += quant_deque[d_i]

    # calculate split factor
    split = (cum_shares + add_shares)/cum_shares
    sf = "{0}:1".format(int(split)) if (split > 1.0) else "1:{0}".format(int(1/split))
    print(args.underlier, " ", sf, " split on", df_und_bs.loc[idx]['Activity Date'])

    # update bucket quantities
    for d_i in range(len(quant_deque)):
      quant_deque[d_i] *= split

  elif (df_und_bs.loc[idx]['Trans Code'] == "ACATS"):
    # handle ACATS transfers
    # *NOTE* This code re-orders the FIFO based on purchase dates.
    #        Must add manual line item to src file w/ Trans_Code = ACATS.
    #        Buy line items then added underneath ACATS line (with external 
    #        sales already deducted and accounted for)

    # build df from deques
    df_tmp = pd.DataFrame([list(date_deque), list(quant_deque), list(notional_deque)]).transpose()

    # rearrange df by date
    df_date_tmp = df_tmp[0].str.split('/',expand=True)
    df_tmp = pd.concat([df_date_tmp[0].apply(pd.to_numeric), df_date_tmp[1].apply(pd.to_numeric), df_date_tmp[2].apply(pd.to_numeric), df_tmp[0], df_tmp[1], df_tmp[2]], axis=1, keys=[0,1,2,3,4,5])
    df_tmp = df_tmp.sort_values(by=[2,0,1])
    df_tmp = df_tmp.reset_index()
    df_tmp = df_tmp.drop(columns=['index'])

    # build updated deques
    date_deque = deque("")
    quant_deque = deque()
    notional_deque = deque()
    for idx2 in df_tmp.index:
      date_deque.append(df_tmp.loc[idx2][3])
      quant_deque.append(df_tmp.loc[idx2][4])
      notional_deque.append(df_tmp.loc[idx2][5])




