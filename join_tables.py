#!/usr/bin/env python


'''
Join mlst2 or abricate/resistome tables from nullarbor runs.
Email: dr.mark.schultz@gmail.com
Github: https://github.com/schultzm
YYYMMDD_HHMM: 20160712_2100
'''


import argparse
import pandas as pd


#set up the arguments parser to deal with the command line input
PARSER = argparse.ArgumentParser(description='Merge mlst2 or abricate tables.')
PARSER.add_argument('-f', '--table_files', help='Feed me tables!',
                    nargs='+', required=True)
PARSER.add_argument('-t', '--table_type', help='Are the tables "mlst2" or\
                    "abricate"?', required=True)
PARSER.add_argument('-o', '--outfile_name', help='Name of outfile.',
                    required=True)
ARGS = PARSER.parse_args()


def main():
    '''
    This main function will read in each table file, run create_frame on each
    infile, store each frame in a list, concatenate the list to a new
    dataframe, clean the dataframe, print the data frame and then write it
    to file.
    '''
    #set file delimiter to comma or tab.
    if 'm' in ARGS.table_type.lower():
        delimiter = '\t'
    else:
        delimiter = ','

    #Frames will store the 2D array (list of tables).
    frames = []
    for filename in ARGS.table_files:
        frame = create_frame(filename, delimiter)
        frames.append(frame)
    #Concatenate the tables stored in frames.
    result = pd.concat(frames[0:], ignore_index=True)
    #Clean the 'result' data-frame.
    #If abricate, do:
    if 'a' in ARGS.table_type.lower():
        #Get rid of whitepace
        result.rename(columns=lambda x: x.strip(), inplace=True)
        result = result[~result['Isolate'].str.contains('DUMMY')]
        result.insert(0, 'Iso', result['Isolate'])
        result.drop(['Isolate'], axis=1, inplace=True)
        result.rename(columns={'Iso':'Isolates'}, inplace=True)
        result.fillna('.', inplace=True)
        result.replace('.', 'No', inplace=True)
        result.replace('&#10004;', 'Yes', inplace=True)
        result.replace('?', 'Maybe', inplace=True)
    else:
        result = result[~result[0].str.contains('DUMMY')]
        result = result.apply(lambda x: x.str.replace('/contigs.fa', ''))
    #Set your joined table free to the screen
    print result
    #Preserve your new creation by writing it to the outfile.
    result.to_csv(ARGS.outfile_name, mode='w', index=False)


def create_frame(filename, delimiter):
    '''
    Create a pandas data frame from an input file.
    '''
    if ARGS.table_type.lower() == 'abricate':
        header_row = 0
    else:
        header_row = None
    file_contents = pd.read_table(filename, sep=delimiter, header=header_row)
    return file_contents


if __name__ == '__main__':
    main()
