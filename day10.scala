import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner

object Day10 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day10-Partitioning")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    // Show only actual errors
    sc.setLogLevel("ERROR")

    println("\n======================================")
    println("       DAY 10 - PARTITIONING")
    println("======================================")

    // ------------------------------------------------
    // 1. Create RDD with 10 values
    // ------------------------------------------------

    val numbers = sc.parallelize(
      Seq(10, 20, 30, 40, 50, 60, 70, 80, 90, 100),
      2
    )

    println("\n1. Original RDD")
    println("Values:")
    println(numbers.collect().mkString(", "))
    println("Number of partitions: " +
      numbers.getNumPartitions)

    // ------------------------------------------------
    // 2. Repartition
    // ------------------------------------------------

    val repartitioned = numbers.repartition(4)

    println("\n2. After repartition(4)")
    println("Values:")
    println(repartitioned.collect().mkString(", "))
    println("Number of partitions: " +
      repartitioned.getNumPartitions)

    // ------------------------------------------------
    // 3. Coalesce
    // ------------------------------------------------

    val coalesced = repartitioned.coalesce(2)

    println("\n3. After coalesce(2)")
    println("Values:")
    println(coalesced.collect().mkString(", "))
    println("Number of partitions: " +
      coalesced.getNumPartitions)

    // ------------------------------------------------
    // 4. Pair RDD with 10 sales records
    // ------------------------------------------------

    val sales = sc.parallelize(
      Seq(
        ("Laptop", 50000),
        ("Mobile", 20000),
        ("Laptop", 30000),
        ("Tablet", 15000),
        ("Mobile", 10000),
        ("Laptop", 20000),
        ("Tablet", 25000),
        ("Monitor", 12000),
        ("Keyboard", 3000),
        ("Mouse", 1500)
      ),
      2
    )

    println("\n4. Pair RDD - 10 Sales Records")

    sales.collect().foreach {
      case (product, amount) =>
        println(product + " -> " + amount)
    }

    println("Original partitions: " +
      sales.getNumPartitions)

    // ------------------------------------------------
    // 5. PartitionBy
    // ------------------------------------------------

    val partitionedSales =
      sales.partitionBy(new HashPartitioner(3))

    println("\n5. After partitionBy(HashPartitioner(3))")
    println("Number of partitions: " +
      partitionedSales.getNumPartitions)

    // ------------------------------------------------
    // 6. Revenue by Product
    // ------------------------------------------------

    val revenueByProduct =
      partitionedSales.reduceByKey(_ + _)

    println("\n6. Revenue By Product")

    revenueByProduct.collect().foreach {
      case (product, revenue) =>
        println(product + " -> " + revenue)
    }

    // ------------------------------------------------
    // 7. Partition information
    // ------------------------------------------------

    println("\n7. Partition Information")

    println("Total partitions: " +
      revenueByProduct.getNumPartitions)

    val partitionSizes =
      revenueByProduct
        .mapPartitions(iter => Iterator(iter.size))
        .collect()

    partitionSizes.zipWithIndex.foreach {
      case (size, index) =>
        println(
          "Partition " + index +
          " contains " + size + " records"
        )
    }

    // ------------------------------------------------
    // 8. Repartition vs Coalesce
    // ------------------------------------------------

    println("\n8. Repartition vs Coalesce")

    println("Repartition:")
    println("- Can increase or decrease partitions")
    println("- Causes a shuffle")

    println("\nCoalesce:")
    println("- Mainly used to decrease partitions")
    println("- Usually avoids a full shuffle")

    // ------------------------------------------------
    // 9. Final
    // ------------------------------------------------

    println("\n======================================")
    println("       DAY 10 COMPLETED")
    println("======================================")

    spark.stop()
  }
}
