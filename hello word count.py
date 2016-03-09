

from operator import add

f = sc.textFile("data/README.md")

word_space = f.flatMap(lambda x: x.split(' '))

word_space.saveAsTextFile("outputs/py_word_space.txt")

words_mapped = word_space.map(lambda x: (x, 1))

words_mapped.saveAsTextFile("outputs/py_words_mapped.txt")

words_reduced = words_mapped.reduceByKey(add)

words_reduced.saveAsTextFile("outputs/py_words_reduced.txt")