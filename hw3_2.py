from pyspark import SparkContext
import sys
import re

# Function to normalize text by removing punctuation and converting to lower case
def normalize_text(line):
    line = line.lower()
    line = re.sub(r'[^a-z\s]', '', line)  # Keep only letters and spaces
    return line.strip()

# Function to load stopwords from a file and return them
def load_stopwords(filepath):
    with open(filepath, 'r') as file:
        stopwords = set(file.read().splitlines())
    return stopwords

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: hw3_2.py <input> <stopwords> <output>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Top10WordsShakespeare")

    # Arguments for the program
    input_path = sys.argv[1]
    stopwords_path = sys.argv[2]
    output_path = sys.argv[3]

    # Load stopwords
    stopwords = load_stopwords(stopwords_path)

    lines = sc.textFile(input_path)
    
    # Make the lines the desired format
    normalized_lines = lines.map(normalize_text)
    
    # Split the lines to get individual words
    words = normalized_lines.flatMap(lambda line: line.split())
    
    # Filter out stop words and empty strings
    filtered_words = words.filter(lambda word: word not in stopwords and word != '')

    # Map words to a tuple and reduce by to count instances
    word_counts = filtered_words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    
    # Take the top 10 results 
    top_10_words = word_counts.takeOrdered(10, key=lambda x: -x[1])

    sc.parallelize(top_10_words).saveAsTextFile(output_path)

    sc.stop()