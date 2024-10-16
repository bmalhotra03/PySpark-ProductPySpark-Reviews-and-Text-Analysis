from pyspark import SparkContext
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: hw3_1.py <input> <output>", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="TotalReviewsAndAverageRate")
    
    # Take arguments from command line for input output
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    lines = sc.textFile(input_path)
    
    # Split file by comma and retrieve fields, pair key and value as per required
    product_reviews = lines.map(lambda line: line.split(",")).map(lambda parts: (parts[0], (1, float(parts[2]))))
    
    # Group all records by their key 
    product_totals = product_reviews.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    
    # Calculate averages for products
    product_averages = product_totals.mapValues(lambda x: (x[0], x[1] / x[0]))
    
    # Sort in ascending order
    sorted_products = product_averages.sortByKey()
    
    sorted_products.saveAsTextFile(output_path)
    
    sc.stop()