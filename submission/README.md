# Distributed System Programing: Assignment 2

### Noam Argaman, username: noamarg@post.bgu.ac.il, id: 322985979
### Shalev Kayat, username: kayatsha@post.bgu.ac.il, id: 211616701

## Running the project
- To run the project, after all the steps' jars have been uploaded, run the main function in the MainClass class.
- Each jar is complied with maven and supposed to be uploaded to s3 bucket.

## Implementation

### The project is implemented as follows: 

- The project composed of 6 steps.
- In the first step, each unique ngram is summed, in addition to the number of words in the corpus.
- In the next 4 steps, the program attach to each unique 3gram its N1, C1, C2, N2. This is achieved by sorting all the 3grams and the corresponding component for the probability formula. 
- In the 5th step, The probabilities are calculated for each 3gram, using its attached ngrams and their occurrences.
- In the 6th and last step, the 3grams are sorted by w1w2 ascending and by the probability for w3 descending.
- The output of the 6th step is the program final output.

## Output directory on s3: s3://dsp2/output