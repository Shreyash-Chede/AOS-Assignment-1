import os
import sys
import re
import threading
import glob
import multiprocessing
import string
import ast


# Define required variable.
inputFilePath = 'D:\Sem1\AOS\Assignment-1\largeTextFile.txt'  # Replace to your desired location
outputDirectoryPath = 'D:\Sem1\AOS\Assignment-1\smallTextFiles' #Replace to your desired location,creating folder smallTextFiles is optional


# The block of code performs check on the existence of input file and the output directory.
def sanityCheck(inputFilePath, outputDirectoryPath):
    if os.path.exists(inputFilePath):
        print("Input file found... checking for output directory")
        if os.path.exists(outputDirectoryPath):
            print("Output directory found, splitting the files")
        else:
            print("Output directory not found creating directory...")
            os.makedirs(outputDirectoryPath)
        return 1
    else:
        print("Input file not found please check inputFilePath variable")
        return 0

# This block of code performs splitting of file by paragraph.
def spilttingLargeText(inputFilePath):
    # Read input file and store value in variable text
    with open(inputFilePath,'r', encoding='utf-8') as inputText:
        text = inputText.read()
    # Split whenever a new paragraph is found in the text
    paragraphContent = re.split('\n\n', text)
    print("Spilt performed and data stored in variable paragraphContent")
    return paragraphContent
    

# This block of code stores separated paragraphs into unique file in a directory.
def smallFileCreation(paragraphContent, outputDirectoryPath):
    # Loop performs the creation of small text file with unique names containing single paragraph.
    for count, paragraph in enumerate(paragraphContent):
        smallTextFile = os.path.join(outputDirectoryPath, f"smallTextFile_{count + 1}.txt")
        with open(smallTextFile, 'w', encoding='utf-8') as smallText:
            smallText.write(paragraph)
        print("Created paragraph file smallTextFile_",count+1,".txt")
    
# Function to count word using threading.
def wordCount(file,processResults):
    with open(file, 'r', encoding='utf-8') as para:
        text = para.read()
        words = text.split()
        for word in words:
            word = word.strip(string.punctuation).lower()
            if word in processResults:
                processResults[word] = processResults[word]+1
            else:
                processResults[word] = 1

# Function to create threads for processes. 
def initiateThreads(chunkOfFiles,i):
    processResults = {}
    threadList = []
    print("Creating Threads...")
    for file in chunkOfFiles:
        thread = threading.Thread(target=wordCount,args=(file, processResults))
        threadList.append(thread)
        thread.start()
    for thread in threadList:
        thread.join()

    print("Storing Process results")
    with open(outputDirectoryPath+"\processResults"+str(i)+".txt", 'w', encoding='utf-8') as file:
        file.write(str(processResults))

# This block of code performs aggregation of counts created by processes
def countAggregation(numberOfProcess):
    totalWordCount = {}
    for i in range(numberOfProcess):
        with open(outputDirectoryPath+"\processResults"+str(i)+".txt", 'r', encoding='utf-8') as file:
            wordDict = ast.literal_eval(file.read())
            for key in wordDict:
                if key in totalWordCount:
                    totalWordCount[key] = totalWordCount[key]+wordDict[key]
                else:
                    totalWordCount[key] = wordDict[key]
    with open(outputDirectoryPath+"\Total_Word_Count.txt", 'w', encoding='utf-8') as file:
        file.write(str(totalWordCount))
    print(totalWordCount)

if __name__ == "__main__":

    # Initialize checks
    sanityCheckPass = sanityCheck(inputFilePath,outputDirectoryPath)
    if sanityCheckPass == 0:
        sys.exit(0)

    # Perform splitting and store each paragraph in one variable
    paragraphContent = spilttingLargeText(inputFilePath)

    # Create small files containing paragraph
    smallFileCreation(paragraphContent,outputDirectoryPath)

    # Store list of all small files in single variable.
    listOfFiles = glob.glob(os.path.join(outputDirectoryPath,"*txt"))
    print("Stored list of files in one variable")

    # Initialize number of process and threads.
    print("Inititating number of process...")
    numberOfProcess = 2

    # List to hold process object.
    print("Creating process list... dictonary to store resuts")
    processList = []

    # This block of code creates number of defined processes and joins them.
    for i in range(numberOfProcess):
        # Divide the number of files to number of defined processes.
        createProcess = multiprocessing.Process(target=initiateThreads,args=(listOfFiles[(i*len((listOfFiles))//numberOfProcess):((i+1)*len((listOfFiles))//numberOfProcess)],i))
        processList.append(createProcess)
        createProcess.start()
    
    for process in processList:
        process.join()

    # This block of code creates process to aggregate the counts.
    aggregationProcess = multiprocessing.Process(target=countAggregation, args=(numberOfProcess,))
    aggregationProcess.start()
    aggregationProcess.join()