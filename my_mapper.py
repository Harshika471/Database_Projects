#!/usr/bin/python

# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Pythozoon interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line character
    line = line.replace("\n", "")

    # 3. We split the line by tabulator characters
    params = line.split(";")

    # 4. We assign res
    res = (params[0],
           params[1],
           int(params[2])
          )

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(my_input_stream, my_output_stream, my_mapper_input_parameters):
    # 0. There are no 'my_mapper_input_parameters' in this exercise, so just ignore the function parameter.
    #    Thus, you must use 'my_input_stream' and 'my_output_stream'.
    #    However, you must not use 'my_mapper_input_parameters'.
    pass

    # START COMPLETING YOUR CODE FROM HERE

    # 1. Create an empty dictionary. As you process the info from the file, you will populate it.
    my_dict = {}
    my_lst = []
    # The dictionary will contain as keys the 'projects' you are reading on the file.
    # Every key will has as its associated value the sum of 'page_views' associated to this project.
    for line in my_input_stream:
        param = process_line(line)
        param = list(param)
        new_var = param[0].split(".")[0]

    # 2. Read the file, line by line.
        if new_var in my_dict:
            my_tuple = my_dict[new_var]+param[2]
            my_dict[new_var] = my_tuple
        else:
            my_dict[new_var] = param[2]

    # 2.1. You can use the auxiliary function "process_line" if you want.
    # Given 1 line from the file, the function will return you the 'project', 'page' and 'num_views' from it.
    # get the words from the line

    # Here words = [en],[en.m],[title of page],[num of views]
    # 2.2. Unfortunately the 'project' might contain one or more dots
    # (e.g., the project 'en' contains no dots, but the projects 'en.b' and 'en.m.voy' do).
    # For those projects containing dots, you need to keep the substring before the first dot appearance.
    # (e.g., 'en' --> 'en', 'en.b' --> 'en', 'en.m.voy' --> 'en').

        # my_words[0] = 'en' not en.m
    # 2.3. Update the dictionary content with the info you have processed from the line.
    # Case 1 --> If the project is not in the dictionary, insert 'project' as a new key with 'num_views' as its associated value.


    # Case 2 --> If the project is already in the dictionary, update the 'project' key by adding 'num_views' to whatever associated value it did contain previously.


    # 3. Print the content of the dictionary to my_output_stream
    for new_var in my_dict:
        my_output_stream.write(new_var + "\t" + "(" + str(my_dict[new_var]) + ")" + "\n")
    # 4. We populate my_output_stream with the (key, value) pairs

        # 4.1. We generate the String to be printed

        # 4.2. We print it


    # Traverse all the keys of the dictionary.
    # Given 1 key, print 1 line with the following format:
    # key \t ( value ) \n

    # STOP WRITING CODE HERE
