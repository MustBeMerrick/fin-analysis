import argparse
import csv
import pathlib
import pandas as pd
from collections import deque
import locale
import math
locale.setlocale( locale.LC_ALL, '' )

# settings
pd.set_option('display.max_rows', 5000)

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

# Filter dataframe for only transactions of underlier
def build_df_for_und(df, underlier):
  df_und=df[df['Instrument'] == underlier]
  
  # fetch all buys/sells/splits/ACATS/options for desired underlier
  df_und = df_und[(df_und['Trans Code'] ==   "Buy") | 
                  (df_und['Trans Code'] ==  "Sell") | 
                  (df_und['Trans Code'] ==   "SPL") |
                  (df_und['Trans Code'] == "ACATS") |
                  (df_und['Trans Code'] ==   "STO") | 
                  (df_und['Trans Code'] ==   "STC") |
                  (df_und['Trans Code'] ==   "BTO") |
                  (df_und['Trans Code'] ==   "BTC") |
                  (df_und['Trans Code'] == "OASGN")]
  df_und = df_und.reset_index()
  df_und = df_und.drop(columns=['index'])
  df_und['Amount'] = df_und['Amount'].replace(to_replace="\((\$.+\.[0-9][0-9])\)", value=r'-\1',   regex=True)
  df_und['Amount'] = df_und['Amount'].replace(to_replace=[',', '\$'],              value=['', ''], regex=True)

  return df_und

# Check if sale is via option assignment
def sale_is_via_call_assignment(df_und, idx):
  via_assignment_str = ""
  proceeds_adj_str = ""
  is_via_assignment=False
  prems=0.0
  if (" Assigned" in str(df_und.loc[idx]['Description'])):
    is_via_assignment=True
    via_assignment_str = "(via assignment) "
    proceeds_adj_str = "  Proceeds (adj)"
  
    # build option name
    option_name = args.underlier + " " + df_und.loc[idx]['Activity Date'] + " Call" + " " + df_und.loc[idx]['Price']

    # fetch all (assigned) option series that triggered the sale. Place in df
    # TODO: Need to worry about series that were BTC that did NOT trigger sale? Can aggregate still?
    print("option: {:s}".format(option_name))
    df_option=df_und[df_und["Description"] == option_name]
    df_option = df_option.reset_index()
    df_option = df_option.drop(columns=['index'])
    
    # agreegate call premiums. These are typically added to proceeds of stock sale
    prems = df_option['Amount'].astype(float).sum()

  return is_via_assignment, via_assignment_str, proceeds_adj_str, is_via_assignment, prems

def print_sale_str(is_via_assignment, quantity, via_assignment_str, date, price, notional, prems):
  notional_str = "Notional: {:s}".format(locale.currency(notional, grouping=True))
  if (is_via_assignment):
    notional_adj = locale.currency(notional + prems, grouping=True)
    notional_str = "Notional/Adj: {:s}/{:s}".format(locale.currency(notional, grouping=True), notional_adj)

  sale_str = "Sold {:.5f} shares {:s}on {:s} for {:s}/share ({:s})".format(quantity, \
                                                                           via_assignment_str, \
                                                                           date, \
                                                                           price, \
                                                                           notional_str)

  print("-------------------------------", sale_str, "-------------------------------")

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
df_und = build_df_for_und(df, args.underlier)

if args.debug:
  print(df_und)
  print()

# iterate over all transactions
for idx in reversed(df_und.index):
  if (df_und.loc[idx]['Trans Code'] == "Buy"):
    # buy = push to FIFO
    fifo_push(df_und.loc[idx]['Activity Date'], float(df_und.loc[idx]['Quantity']), float(df_und.loc[idx]['Amount']))
  elif (df_und.loc[idx]['Trans Code'] == "Sell"):
    # sell = pop from FIFO
    
    # check if sale was via call assignment
    is_via_assignment, via_assignment_str, proceeds_adj_str, is_via_assignment, prems = sale_is_via_call_assignment(df_und, idx)

    print_sale_str(is_via_assignment, float(df_und.loc[idx]['Quantity']), via_assignment_str, df_und.loc[idx]['Activity Date'], df_und.loc[idx]['Price'], float(df_und.loc[idx]['Amount']), prems)
    print("     Security   Date Sold    Quantity    Proceeds" + proceeds_adj_str + "   Date Acquired       Price      Cost Basis          PL".format())
    remaining_quantity = float(df_und.loc[idx]['Quantity'])
    proceeds_per_share = float(df_und.loc[idx]['Amount'])/remaining_quantity
    pl_aggr = 0
    proceeds_aggr = 0
    cb_aggr = 0
    while remaining_quantity > 0:
      if (remaining_quantity >= quant_deque[0]):
        # entire bucket will be popped

        # grab date/quantity/amount from the oldest purchase bucket
        buy_date, buy_quantity, buy_notional = fifo_pop()

        # calculate cost basis per share of the bucket
        cb_per_share = buy_notional/buy_quantity

      else:
        # Tail of FIFO will be modified

        # not entire quantity will be ammortized- just remaining quantity
        buy_quantity = remaining_quantity
        buy_date = date_deque[0]

        # calculate cost basis per share of the bucket in use
        cb_per_share = notional_deque[0]/quant_deque[0]

        # update FIFO tails
        fifo_modify_tail(buy_quantity, cb_per_share * buy_quantity)
      
      #endif

      # accumulate P/L, proceeds, cb and update remaining_quantity
      pl_i = (proceeds_per_share + cb_per_share) * buy_quantity
      proceeds_i = proceeds_per_share * buy_quantity
      cb_i = abs(cb_per_share*buy_quantity)
      pl_aggr += pl_i
      proceeds_aggr += proceeds_i
      cb_aggr += cb_i
      remaining_quantity -= buy_quantity

      # print purchase sub-line
      if is_via_assignment:
        acq_print_str = "     {:>8}{:>12}{:>12}{:>12}{:>16}{:>16}{:>12}{:>16}{:>12}".format(args.underlier, df_und.loc[idx]['Activity Date'], "{:.5f}".format(buy_quantity), locale.currency(proceeds_i, grouping=True), "TBD", buy_date, locale.currency(abs(cb_per_share)), locale.currency(cb_i, grouping=True), locale.currency(pl_i, grouping=True))
      else:
        acq_print_str = "     {:>8}{:>12}{:>12}{:>12}{:>16}{:>12}{:>16}{:>12}".format(args.underlier, df_und.loc[idx]['Activity Date'], "{:.5f}".format(buy_quantity), locale.currency(proceeds_i, grouping=True), buy_date, locale.currency(abs(cb_per_share)), locale.currency(cb_i, grouping=True), locale.currency(pl_i, grouping=True))
      
      print(acq_print_str)

    print("---------------------------------------------------------------------------------------------------------------------------")
    if is_via_assignment:
      total_print_str="Total:{:>7}{:>12}{:>12}{:>12}{:>16}{:>16}{:>12}{:>16}{:>12}".format("", "", "", locale.currency(proceeds_aggr, grouping=True), "", "", "", locale.currency(cb_aggr, grouping=True), locale.currency(pl_aggr, grouping=True))
    else:
      total_print_str="Total:{:>7}{:>12}{:>12}{:>12}{:>16}{:>12}{:>16}{:>12}".format("", "", "", locale.currency(proceeds_aggr, grouping=True), "", "", locale.currency(cb_aggr, grouping=True), locale.currency(pl_aggr, grouping=True))

    print(total_print_str)
    print()

  elif (df_und.loc[idx]['Trans Code'] == "SPL"):
    # handle stock split

    cum_shares = 0
    add_shares = float(df_und.loc[idx]['Quantity'])
    
    # calculate total shares owned
    for d_i in range(len(quant_deque)):
      cum_shares += quant_deque[d_i]

    # calculate split factor
    split = (cum_shares + add_shares)/cum_shares
    sf = "{0}:1".format(int(split)) if (split > 1.0) else "1:{0}".format(int(1/split))
    print(args.underlier, " ", sf, " split on", df_und.loc[idx]['Activity Date'])

    # update bucket quantities
    for d_i in range(len(quant_deque)):
      quant_deque[d_i] *= split

  elif (df_und.loc[idx]['Trans Code'] == "ACATS"):
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


