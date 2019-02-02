sc.range(3,7)
  .map(num=>{val j = num%2; if(j == 0) num + num - 1 else if(num == 7) num + num - 1 else num})
  .collect()