sc.range(1992,2018)
  .map(year => if(year%2 == 0) year + 1 else year*2)
  .collect()