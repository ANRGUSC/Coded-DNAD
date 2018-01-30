"""
 * Copyright (c) 2017, Autonomous Networks Research Group. All rights reserved.
 *     contributors: 
 *      Pranav Sakulkar, October 2017
 *      Jiatong Wang, October 2017
 *      Aleksandra Knezevic, October 2017
 *      Bhaskar Krishnamachari, October 2017
 *     Read license file in main directory for more details  
"""

import os
import sys
import time

def task(onefile, pathin, pathout):


    filelist=[]
    filelist.append(onefile)

    time.sleep(15)
    num=filelist[0].partition('s')[0]

    with open(os.path.join(pathout, num+'merged_file0.ipsum'),'w') as outfile:
        for filename in filelist:
            with open(os.path.join(pathin, filename), 'r') as infile:
                for line in infile:
                    outfile.write(line)


if __name__ == '__main__':

    filelist = ['25split_0']
    task(filelist, '/home/apac/security_app', '/home/apac/security_app')
