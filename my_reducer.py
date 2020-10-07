#!/usr/bin/python

# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

#---------------------------------------
#  FUNCTION get_key_value
#---------------------------------------
def get_key_value(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line char
    line = line.replace('\n', '')

    # 3. We get the key and value
    words = line.split('\t')
    key = words[0]
    value = int(words[1][1:-1])

    # 4. We assign res
    res = (key, value)

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    # 0. There are no 'my_mapper_input_parameters' in this exercise, so just ignore the function parameter.
    #    Thus, you must use 'my_input_stream' and 'my_output_stream'.
    #    However, you must not use 'my_reducer_input_parameters'.
    pass

    # START COMPLETING YOUR CODE FROM HERE

    # 1. Create an empty dictionary. As you process the info from the file, you will populate it.
    empty_dict = {}
    # # The dictionary will contain as keys the 'projects' you are reading on the file.
    # # Every key will has as its associated value the sum of 'page_views' associated to this project.

    # # 2. Read the file, line by line.
    for lines in my_input_stream:
    # # 2.1. You can use the auxiliary function "get_key_value" if you want.
        (new_letter, new_num_words) = get_key_value(lines)
    # # Given 1 line from the file, the function will return you the 'project' and 'num_views' from it.
        if new_letter in empty_dict:
            my_tuple = empty_dict[new_letter] + new_num_words
            empty_dict[new_letter] = my_tuple
        else:
            empty_dict[new_letter] = new_num_words

    # # 2.2. Update the dictionary content with the info you have processed from the line.

    #
    # # Case 2 --> If the project is already in the dictionary, update the 'project' key by adding 'num_views' to whatever associated value it did contain previously.

    # # 3. Sort the keys of the dictionary by decreasing order in their associated values.
    # (e.g., given 'es' -> 553435 and 'de' -> 1496353 the key 'de' goes first and the key 'es' goes later).
    empty_dict(sorted(empty_dict.items))

    # 4. Print the content of the sorted dictionary to my_output_stream
    for new_letter in empty_dict:
        my_output_stream.write(new_letter + "\t" + "(" + str(empty_dict[new_letter]) + ")" + "\n")
    # Traverse all the keys of the dictionary.
    # Given 1 key, print 1 line with the following format:
    # key \t ( value ) \n

    # STOP WRITING CODE HERE