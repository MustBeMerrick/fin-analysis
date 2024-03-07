#!/bin/sh

while IFS=',' read -ra array; do
  instruments+=("${array[3]}")
done < ../data/robinhood/rh_transactions.csv

for i in "${instruments[@]}"; do
  valid_instr=$(echo $i | grep -Eo "^[A-Z]+")
  if [[ $valid_instr != "" ]]; then
    valid_intrs+=($valid_instr)
  fi
done

uniq_intrs=($(for instr in "${valid_intrs[@]}"; do echo "${instr}"; done | sort -u))

cum_rc=0
for instr_i in "${uniq_intrs[@]}"; do
  rc_i=$(python3 ../scripts/parse_rh_transactions.py -u ${instr_i} 1> /dev/null 2> /dev/null; echo $?)
  cum_rc=$((${cum_rc} | ${rc_i}))
  if [[ ${cum_rc} -ne 0 ]]; then
    echo "Failure for $instr_i"
    break
  fi
done

if [[ $cum_rc -eq 0 && $? -eq 0 ]]; then
  echo "Gate Check PASS"
else
  echo "Gate Check FAIL"
  exit 1
fi