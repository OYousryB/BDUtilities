#!/bin/bash

source scripts/env.txt
source scripts/params.txt

serial_tbl_list="
"

parallel_tbl_list="
catalog_returns
catalog_sales
customer_address
customer
inventory
store_returns
store_sales
web_returns
web_sales
catalog_page
customer_demographics
date_dim
dbgen_version
household_demographics
income_band
item
promotion
reason
ship_mode
store
time_dim
warehouse
web_page
web_site
call_center
"

genTableSerialAll()
{
  local sf=$1
  local dir=$2

  for table in $serial_tbl_list
  do
    genTableSerial $sf $table $dir
  done
}

genTableSerial()
{
  local sf=$1
  local table=$2
  local dir=$3

  echo ./dsdgen -scale $sf -dir $dir -table $table -terminate N
  ./dsdgen -scale $sf -dir $dir -table $table -terminate N &> /dev/null &
}

genTableParallelAll()
{
  local sf=$1
  local chunks=$2
  local dir=$3

  for table in $parallel_tbl_list
  do
    genTableParallel $sf $table $chunks $dir
  done

}

genTableParallel()
{
  local sf=$1
  local table=$2
  local chunks=$3
  local dir=$4

  for i in $(seq 1 $chunks); do
    genTableChunk $sf $table $chunks $i $dir
  done
}

genTableChunk()
{
  local sf=$1
  local table=$2
  local chunks=$3
  local current=$4
  local dir=$5

  echo ./dsdgen -scale $sf -dir $dir  -parallel $chunks -child $current -table $table -terminate N
  ./dsdgen -scale $sf -dir $dir -parallel $chunks -child $current -table $table -terminate N &> /dev/null &
}

mergeHeaderBodyAll()
{
  local dir=$1

  for table in $serial_tbl_list
  do
    mergeHeaderBody $table $dir &
  done
}

mergeHeaderBody()
{
  local table=$1
  local dir=$2

  local header=${TPCDS_EXTRA_INFO}/headers/${table}.txt

  local target=${dir}/${table}.txt
  local source=${dir}/${table}.dat

  if [[ $WITH_HEADERS -eq 1 ]]
  then
    cp $header $target
  fi
  cat $source >> $target
  rm $source
}

mergeHeaderAllBodyChunksAll()
{
  local chunks=$1
  local dir=$2

  for table in $parallel_tbl_list
  do
    mergeHeaderAllBodyChunks $table $chunks $dir &
  done
}

mergeHeaderAllBodyChunks()
{
  local table=$1
  local chunks=$2
  local dir=$3

  local header=${TPCDS_EXTRA_INFO}/headers/${table}.txt
  local target=${dir}/${table}.txt

  if [[ $WITH_HEADERS -eq 1 ]]
  then
    cp $header $target
  fi

  for i in $(seq 1 $chunks); do
    local source=${dir}/${table}_${i}_${chunks}.dat
    if [ -e $source ]; then
      cat $source >> $target
      rm $source
    fi
  done
}

#################################### MAIN ######################################

for SF in $SCALES
do
    OUTPUT=$DATA_DB/tpcds/$SF
    mkdir -p $OUTPUT

    TBL_DIR=$OUTPUT/tbl
    if [[ ! -d $TBL_DIR ]] || [[ $OVERWRITE_TBL_DATA -eq 1 ]]
    then
      rm -rf $TBL_DIR
      mkdir -p $TBL_DIR

      cd $TPCDS_PATH/tools

      start=$SECONDS

      echo "Generating CSV chunks"

      #serial generation
      genTableSerialAll $SF $TBL_DIR

      #parallel generation by chunks
      genTableParallelAll $SF $TPCDS_PARALLELISM $TBL_DIR

      wait

      end1=$SECONDS
      echo CSV chunks generated. Elapsed $(($end1 - $start)) seconds.

      echo "Merging CSV chunks"

      #merge header and body
      mergeHeaderBodyAll $TBL_DIR

      #merge header and all body chunks
      mergeHeaderAllBodyChunksAll $TPCDS_PARALLELISM $TBL_DIR

      wait

      end2=$SECONDS

      echo CSV chunks merged. Elapsed $(($end2 - $end1)) seconds.
      echo Total Elapsed $(($end2 - $start)) seconds.

      echo "Moving DATA to new directory"
        # move to target directories
        mkdir $TBL_DIR/catalog_sales
        mv $TBL_DIR/catalog_sales.txt $TBL_DIR/catalog_sales

        mkdir $TBL_DIR/catalog_returns
        mv $TBL_DIR/catalog_returns.txt $TBL_DIR/catalog_returns

        mkdir $TBL_DIR/inventory
        mv $TBL_DIR/inventory.txt $TBL_DIR/inventory

        mkdir $TBL_DIR/store_sales
        mv $TBL_DIR/store_sales.txt $TBL_DIR/store_sales

        mkdir $TBL_DIR/store_returns
        mv $TBL_DIR/store_returns.txt $TBL_DIR/store_returns

        mkdir $TBL_DIR/web_sales
        mv $TBL_DIR/web_sales.txt $TBL_DIR/web_sales

        mkdir $TBL_DIR/web_returns
        mv $TBL_DIR/web_returns.txt $TBL_DIR/web_returns

        mkdir $TBL_DIR/catalog_page
        mv $TBL_DIR/catalog_page.txt $TBL_DIR/catalog_page

        mkdir $TBL_DIR/customer_address
        mv $TBL_DIR/customer_address.txt $TBL_DIR/customer_address

        mkdir $TBL_DIR/customer_demographics
        mv $TBL_DIR/customer_demographics.txt $TBL_DIR/customer_demographics

        mkdir $TBL_DIR/customer
        mv $TBL_DIR/customer.txt $TBL_DIR/customer

        mkdir $TBL_DIR/date_dim
        mv $TBL_DIR/date_dim.txt $TBL_DIR/date_dim

        mkdir $TBL_DIR/household_demographics
        mv $TBL_DIR/household_demographics.txt $TBL_DIR/household_demographics

        mkdir $TBL_DIR/income_band
        mv $TBL_DIR/income_band.txt $TBL_DIR/income_band

        mkdir $TBL_DIR/item
        mv $TBL_DIR/item.txt $TBL_DIR/item

        mkdir $TBL_DIR/promotion
        mv $TBL_DIR/promotion.txt $TBL_DIR/promotion

        mkdir $TBL_DIR/reason
        mv $TBL_DIR/reason.txt $TBL_DIR/reason

        mkdir $TBL_DIR/ship_mode
        mv $TBL_DIR/ship_mode.txt $TBL_DIR/ship_mode

        mkdir $TBL_DIR/store
        mv $TBL_DIR/store.txt $TBL_DIR/store

        mkdir $TBL_DIR/time_dim
        mv $TBL_DIR/time_dim.txt $TBL_DIR/time_dim

        mkdir $TBL_DIR/warehouse
        mv $TBL_DIR/warehouse.txt $TBL_DIR/warehouse

        mkdir $TBL_DIR/web_page
        mv $TBL_DIR/web_page.txt $TBL_DIR/web_page

        mkdir $TBL_DIR/web_site
        mv $TBL_DIR/web_site.txt $TBL_DIR/web_site

        mkdir $TBL_DIR/call_center
        mv $TBL_DIR/call_center.txt $TBL_DIR/call_center

        mkdir $TBL_DIR/dbgen_version
        mv $TBL_DIR/dbgen_version.txt $TBL_DIR/dbgen_version
     else
      echo "TPC-DS data is already generated in ${TBL_DIR} either delete this directory or set OVERWRITE_TBL_DATA=1 variable"
    fi

done
