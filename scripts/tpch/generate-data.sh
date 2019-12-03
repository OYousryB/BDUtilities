#!/bin/bash

source scripts/env.txt
source scripts/params.txt

genTable()
{
  local sf=$1
  local table=$2

  echo ./dbgen -vf -s $sf -T $table
  ./dbgen -vf -s $sf -T $table &> /dev/null &
}

genTablePara()
{
  local sf=$1
  local table=$2
  local chunks=$3

  for i in $(seq 1 $chunks); do
    genTableChunk $sf $table $chunks $i
  done
}

genTableChunk()
{
  local sf=$1
  local table=$2
  local chunks=$3
  local current=$4

  echo ./dbgen -vf -s $sf -C $chunks -S $current -T $table
  ./dbgen -vf -s $sf -C $chunks -S $current -T $table &> /dev/null &
}

mergeHeaderBody()
{
  local table=$1

  local header=${TPCH_EXTRA_INFO}/headers/${table}.txt
  local target=${TPCH_PATH}/dbgen/${table}.txt
  local source=${TPCH_PATH}/dbgen/${table}.tbl

   if [[ $WITH_HEADERS -eq 1 ]]
   then
    cp $header $target
   fi
  cat $source >> $target
  rm $source
}

mergeHeaderAllBodyChunks()
{
  local table=$1
  local chunks=$2

  local header=${TPCH_EXTRA_INFO}/headers/${table}.txt
  local target=${TPCH_PATH}/dbgen/${table}.txt
   if [[ $WITH_HEADERS -eq 1 ]]
   then
    cp $header $target
   fi
  for i in $(seq 1 $chunks); do
    local source=${TPCH_PATH}/dbgen/${table}.tbl.$i
    if [ -e $source ]; then
      cat $source >> $target
      rm $source
    fi
  done
}

for SF in $SCALES
do
  OUTPUT=$DATA_DB/tpch/$SF
  # Clean working directory
  mkdir -p $OUTPUT

  TBL_DIR=$OUTPUT/tbl

  if [[ ! -d $TBL_DIR ]] || [[ $OVERWRITE_TBL_DATA -eq 1 ]]
  then
    cd $TPCH_PATH/dbgen

    rm -rf $TBL_DIR
    mkdir -p $TBL_DIR

    start=$SECONDS

    echo "Generating CSV chunks"

    #serial generation
    genTable $SF l    #nation/region

    #parallel generation by chunks
    genTablePara $SF o $TPCH_PARALLELISM    #orders/lineitem
    genTablePara $SF p $TPCH_PARALLELISM   #part/partsupp
    genTablePara $SF c $TPCH_PARALLELISM   #customer
    genTablePara $SF s $TPCH_PARALLELISM   #supplier
    wait
    endGen=$SECONDS

    echo "Merging CSV chunks"

    #merge header and body
    mergeHeaderBody nation &
    mergeHeaderBody region &

    #merge header and body chunks
    mergeHeaderAllBodyChunks lineitem $TPCH_PARALLELISM &
    mergeHeaderAllBodyChunks orders $TPCH_PARALLELISM &
    mergeHeaderAllBodyChunks partsupp $TPCH_PARALLELISM &
    mergeHeaderAllBodyChunks part $TPCH_PARALLELISM &
    mergeHeaderAllBodyChunks customer $TPCH_PARALLELISM &
    mergeHeaderAllBodyChunks supplier $TPCH_PARALLELISM &

    wait

    # move to target directories
    mkdir $TBL_DIR/nation
    mv nation.* $TBL_DIR/nation

    mkdir $TBL_DIR/region
    mv region.* $TBL_DIR/region

    mkdir $TBL_DIR/lineitem
    mv lineitem.* $TBL_DIR/lineitem

    mkdir $TBL_DIR/orders
    mv orders.* $TBL_DIR/orders

    mkdir $TBL_DIR/supplier
    mv supplier.* $TBL_DIR/supplier

    mkdir $TBL_DIR/part
    mv part.* $TBL_DIR/part

    mkdir $TBL_DIR/partsupp
    mv partsupp.* $TBL_DIR/partsupp

    mkdir $TBL_DIR/customer
    mv customer.* $TBL_DIR/customer

    endMerge=$SECONDS
    echo Generation time $(($endGen - $start)) seconds.
    echo Merging time $(($endMerge - $endGen)) seconds.

    cd ../
  else
    echo "Tpch data is already generated in ${TBL_DIR} either delete this directory or set OVERWRITE_TBL_DATA=1 variable"
  fi
done