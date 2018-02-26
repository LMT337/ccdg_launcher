import argparse, csv, os, webbrowser, datetime, glob

#generate date month/day/year string
mm_dd_yy = datetime.datetime.now().strftime("%m%d%y")

#set working dir to ccdg woid dir when deployed
working_dir = os.getcwd()

def main():
    desc_str = """
        Program to create sample emails and update sample launch status in tracking spreadsheets.
    """

    parser = argparse.ArgumentParser(description=desc_str)

    group = parser.add_mutually_exclusive_group()

    group.add_argument("-f", type=str, help='Input compute workflow file with all samples')
    group.add_argument('-l', help='Link to links compute workflow file', action='store_true')
    #add group for woid only processing

    args = parser.parse_args()

    if args.l:
        generate_compute_workflow()
    if args.f:
        woid_list()
        samples = sample_email(working_dir + '/' + args.f)
        # print(samples)
        # sample_launch(args.f, samples)


def generate_compute_workflow():

    firefox_path = '/gapp/ia32linux/bin/firefox %s'
    launch_link = 'https://imp-lims.gsc.wustl.edu/entity/compute-workflow?_show_result_set_definition=1&_result_set_name=&cw_id=&creation_event_id=&assay_id=&assay_id=&protocol_id=901&date_scheduled=&sample_full_name=&number_of_input=&number_of_instrument_data='
    open_url = input('Open compute workflow in firefox? (y/n)\n')
    while open_url not in ('y', 'n'):
        open_url = input('Please enter y or n:\n')
    if open_url == 'y':
        print('here!!')
        webbrowser.get(firefox_path).open(launch_link)

    print('\nCompute workflow link:\n{}'.format(launch_link))
    print('\nCreate workflow file:\ncat > ccdg.computeworkflow.{}.tsv\n'.format(mm_dd_yy))


#set dir to ccdg woid dir
#make list of all woid dirs, filter out anything that isn't a woid
def woid_list():
    woid_dirs = []
    def is_int(string):
        try:
            int(string)
        except ValueError:
            return False
        else:
            return True

    woid_dir_unfiltered = glob.glob('285*')

    for woid in filter(is_int, woid_dir_unfiltered):
        woid_dirs.append(woid)
    print(woid_dirs)

    return woid_dirs

def compute_workflow_create(compute_workflow_all_file, woid):
    outfile = woid + '.compute.workflow.' + mm_dd_yy + '.tsv'
    with open(compute_workflow_all_file, 'r') as compute_workflow_all_csv, open(outfile, 'w') as outfilecsv:

        cwl_reader = csv.DictReader(compute_workflow_all_csv, delimiter='\t')
        cwl_fieldnames = cwl_reader.fieldnames

        outfile_writer = csv.DictWriter(outfilecsv, fieldnames=cwl_fieldnames, delimiter='\t')
        outfile_writer.writeheader()
        for line in cwl_reader:
            if woid in line['Work Order']:
                outfile_writer.writerow(line)
    return




#sample email function, create new woid dir if it doesn't exist and master spreadsheet if it doesn't exist.
#write emails to email file
def sample_email(infile):

    sample_list = []

    while True:
        woid = input('----------\nWork order id (enter to exit):\n').strip()
        # if not woid:
        if (len(woid) == 0):
            print('Exiting sample email creation')
            break
        try:
            val = int(woid)
        except ValueError:
            print("\nwoid must be a number.")
            continue

        #create woid dir if it does not exist
        if not os.path.exists(woid):
            print('\n{} dir does not exist.'.format(woid))
            create_woid = input('Create {} directory? (y or n)\n'.format(woid))
            if create_woid == 'y':
                os.makedirs(woid)
                os.chdir(woid)
                #create master file if it does not exist
                create_master = input('\nCreate {}.master.samples.tsv file? (y or n)\n'.format(woid))
                if create_master == 'y':
                    master_sample_link = 'https://imp-lims.gsc.wustl.edu/entity/setup-work-order/' + woid + \
                                         '?perspective=Sample&setup_name=' + woid
                    print('\nMaster sample link:\n{}\nInput samples:'.format(master_sample_link))
                    master_samples = []
                    while True:
                        master_sample_line = input()
                        if master_sample_line:
                            master_samples.append(master_sample_line)
                        else:
                            break
                    master_outfile = woid + '.master.samples.tsv'
                    qc_tracking_file = woid + '.qcstatus.tsv'
                    with open(master_outfile, 'w') as mastercsv, open(qc_tracking_file, 'w') as statuscsv:
                        master_write = csv.writer(mastercsv, delimiter='\n')
                        status_write = csv.writer(statuscsv, delimiter='\n')
                        master_write.writerows([master_samples])
                        status_write.writerows([master_samples])
                os.chdir(working_dir)

                if create_master == 'n':
                    pass
            if create_woid == 'n':
                continue

        sample_number = input('\nSample number:\n')
        try:
            val = int(sample_number)
        except ValueError:
            print('Sample number must be a number.')
            exit()

        date = input('\nUse today\'s date? (y or n)\n')
        if date == 'y':
            sample_outfile = sample_number + 'samples.' + mm_dd_yy
        if date == 'n':
            new_date = input('Input date (MMDDYY):\n')
            try:
                val = int(new_date)
            except ValueError:
                print('Date must be a number')
                exit()
            sample_outfile = sample_number + 'samples.' + new_date

        if os.path.exists(woid + '/' + sample_outfile):
            print('{} exists, please create file manually\n(sample #)samples.(email date)'.format(sample_outfile))
            continue

        print('\nProduction samples (from email):')
        sample_info = []
        while True:
            sample_line = input()
            if sample_line:
                sample_info.append(sample_line)
            else:
                break

        os.chdir(woid)

        #write samples to email file
        with open(sample_outfile, 'w') as sample_outfilecsv:
            sample_write = csv.writer(sample_outfilecsv, delimiter='\n')
            sample_write.writerows([sample_info])

        with open(sample_outfile, 'r') as samplecsv:

            sample_reader = csv.DictReader(samplecsv, delimiter='\t')

            for sample_line in sample_reader:
                sample_remove_whitespace = {k.replace(' ', ''): v for k, v in sample_line.items()}
                sample_name = sample_remove_whitespace['Library']
                sample_name_split = sample_name.split('-')[0]
                sample = sample_name_split.strip()
                sample_list.append(sample)

        compute_workflow_create(infile, woid)
        quit()

    return sample_list


#ccdg status fuction, update samples status in status file using launch emails and input file
#open input file and add it to dict with sample as key
#open email file, if sample name found in input file then check launch values if equal assign launch
#satus, if not equal assign failed status, store in fail file.
qc_fieldnames = ['QC Sample', 'WOID', 'PSE', 'Instrument Check', 'Launch Status', 'Launch Date', '# of Inputs',
             '# of Instrument Data', 'QC Status', 'QC Date', 'QC Failed Metrics', 'COD Collaborator', 'QC Directory']

def sample_launch(infile, sample_list):
    infile_sample_dict = {}
    with open(infile) as infiletsv:
        infile_reader = csv.DictReader(infiletsv, delimiter='\t')
        outfile_header = infile_reader.fieldnames + qc_fieldnames
        for line in infile_reader:
            print(line)


if __name__ == '__main__':
    main()

