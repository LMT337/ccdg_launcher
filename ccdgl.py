import argparse, csv, os, webbrowser, datetime, glob

#generate date month/day/year string
mm_dd_yy = datetime.datetime.now().strftime("%m%d%y")

#set working dir to ccdg woid dir when deployed
working_dir = os.getcwd()
# working_dir  = '/gscmnt/gc2783/qc/CCDG-build38/CCDG-FINRISK/Dev'

qc_fieldnames = ['WOID','QC Sample','PSE','# of Inputs','# of Instrument Data','LIMS Status','Instrument Check',
                 'Launch Status','Launch Date','QC Status','QC Date','QC Failed Metrics','COD Collaborator',
                 'QC Directory','Top Up']

def main():
    desc_str = """
        Program to create sample emails and update sample launch status in tracking spreadsheets.
    """

    parser = argparse.ArgumentParser(description=desc_str)

    group = parser.add_mutually_exclusive_group()

    group.add_argument("-f", type=str, help='Input compute workflow file with all samples')
    group.add_argument('-l', help='Link to links compute workflow file', action='store_true')
    #TODO add group for woid only processing

    args = parser.parse_args()

    if args.l:
        generate_compute_workflow()
        quit()

    if not os.path.exists(args.f):
        print('{} file not found.'.format(args.f))

    if os.path.exists(args.f):
        woid_dirs = woid_list()
        ccdg_launcher(working_dir + '/' + args.f)

#print link to download compute workflow from lims and cat command
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

    return woid_dirs

#create computeworkflow file in woid directory
def compute_workflow_create(compute_workflow_all_file, woid):
    if not os.path.exists(working_dir + '/' + woid):
        os.chdir(woid)

    outfile = woid + '.compute.workflow.' + mm_dd_yy + '.tsv'
    with open(compute_workflow_all_file, 'r') as compute_workflow_all_csv, open(outfile, 'w') as outfilecsv:

        cwl_reader = csv.DictReader(compute_workflow_all_csv, delimiter='\t')
        cwl_fieldnames = cwl_reader.fieldnames

        outfile_writer = csv.DictWriter(outfilecsv, fieldnames=cwl_fieldnames, delimiter='\t')
        outfile_writer.writeheader()

        for line in cwl_reader:
            if (woid in line['Work Order']) and 'Aligned Bam To BQSR Cram And VCF Without Genotype' in line['Protocol']:
                outfile_writer.writerow(line)
    return outfile

#Check compute workflow file to see if all samples are there
sample_not_found = []
def cw_sample_check(infile, sample):

    cw_samples = []

    with open(infile) as infiletsv:
        infile_reader = csv.DictReader(infiletsv, delimiter='\t')
        for qc_data in infile_reader:
            cw_samples.append(qc_data['Sample Full Name'])

    if sample not in cw_samples:
        sample_not_found.append(sample)
    return sample_not_found

#assing qc fields, check instrument match and sample lims status
def sample_pse_match(infile, sample, woid):

    qc_results = {}

    with open(infile) as infiletsv:
        infile_reader = csv.DictReader(infiletsv, delimiter='\t')
        for qc_data in infile_reader:
            if sample in qc_data['Sample Full Name']:
                # print('Sample: {} found to qc'.format(sample))
                if sample == qc_data['Sample Full Name']:
                    qc_results['QC Sample'] = qc_data['Sample Full Name']
                    qc_results['PSE'] = qc_data['PSE']
                    qc_results['# of Inputs'] = qc_data['# of Inputs']
                    qc_results['# of Instrument Data'] = qc_data['# of Instrument Data']
                    qc_results['Launch Date'] = mm_dd_yy
                    qc_results['WOID'] = woid
                    qc_results['QC Status'] = 'NONE'
                    qc_results['QC Date'] = 'NONE'
                    qc_results['QC Failed Metrics'] = 'NONE'
                    qc_results['COD Collaborator'] = 'NONE'
                    qc_results['QC Directory'] = 'NONE'
                    qc_results['LIMS Status'] = qc_data['Status']

                    if (int(qc_data['# of Inputs']) == int(qc_data['# of Instrument Data'])) and qc_data['Status'] == \
                            'ready':
                        qc_results['Instrument Check'] = 'PASS'
                        qc_results['Launch Status'] = 'Launched'
                    elif (int(qc_data['# of Inputs']) == int(qc_data['# of Instrument Data'])) and qc_data['Status'] ==\
                            'active':
                        qc_results['Instrument Check'] = 'PASS'
                        qc_results['Launch Status'] = 'MANUAL LAUNCH'
                        qc_results['Launch Date'] = 'NA'
                    else:
                        qc_results['Instrument Check'] = 'FAIL'
                        qc_results['Launch Status'] = 'DO NOT LAUNCH'
                        qc_results['Launch Date'] = 'NA'

    return qc_results

#print sample status results
def output_launcher_results(pse_ready, pse_fail, pse_fail_ready, pse_fail_fail, ins_pass_stat_actv):

    if len(pse_ready) > 0:
        print('\n{} Samples pass instrument check, status updated to Launched:'.format(str(len(pse_ready))))
        print('Sample', 'PSE', sep='\t')
        for pse in pse_ready:
            print(pse)

    if len(pse_fail) > 0:
        print('\n{} Samples failed instrument check, status is do not launch: '.format(str(len(pse_fail))))
        print('Sample', 'PSE', '# of Inputs', '# of Instrument Data', sep="\t")
        for pse in pse_fail:
            print(pse)

    if len(pse_fail_ready) > 0:
        print('\n{} Failed samples, pass instrument check, status updated to Launched:'.format(str(len(pse_fail_ready))))
        print('Sample', 'PSE', sep='\t')
        for pse in pse_fail_ready:
            print(pse)

    if len(pse_fail_fail) > 0:
        print('\n{} Samples failed instrument check again, status is do not launch:'.format(str(len(pse_fail_fail))))
        for pse in pse_fail_fail:
            print(pse)

    if len(ins_pass_stat_actv) > 0:
        print('\n{} Samples pass instrument check, protocol status is active:'.format(str(len(ins_pass_stat_actv))))
        print('Sample', 'PSE', '# of Inputs', '# of Instrument Data', 'LIMS Status', sep="\t")
        for pse in ins_pass_stat_actv:
            print(pse)
        print('Manually launch samples if they have not already been launched (check jira).')

    else:
        print('\nAll processed samples have a launch status.\n')

#process samples update qc files and populate results lists for printing
qc_master_sample_list = []
def qc_status_update(compute_workflow, sample_list, woid, qc_status_file):

    temp_status_file = woid + '.qcstatus.temp.tsv'
    launch_failed_temp = woid + '.launch.fail.temp.tsv'
    launch_failed_file = woid + '.launch.fail.tsv'
    instrument_pass_status_active_file = woid + '.instrument.pass.status.active.tsv'
    instrument_pass_status_active_file_tmp = woid + '.instrument.pass.status.active.temp.tsv'

    with open(qc_status_file,'r') as qcstatuscsv, open(temp_status_file,'w') as tempstatuscsv, \
            open(launch_failed_temp,'w') as launchtempcsv, open(instrument_pass_status_active_file_tmp,'w') as ipsafcsv:

        status_file_reader = csv.DictReader(qcstatuscsv, delimiter='\t')
        status_file_header = status_file_reader.fieldnames

        temp_status_writer = csv.DictWriter(tempstatuscsv, fieldnames=status_file_header, delimiter='\t')
        temp_status_writer.writeheader()

        temp_launch_fail_writer = csv.DictWriter(launchtempcsv, fieldnames=status_file_header, delimiter='\t')
        temp_launch_fail_writer.writeheader()

        instrument_pass_status_active_writer = csv.DictWriter(ipsafcsv, fieldnames=status_file_header, delimiter='\t')
        instrument_pass_status_active_writer.writeheader()

        #Declare lists for print statements to terminal
        pse_ready = []
        pse_fail = []
        pse_fail_ready = []
        pse_fail_fail = []
        ins_pass_stat_actv = []

        for line in status_file_reader:
            qc_master_sample_list.append(line['Full Name'])

            #check samples that have already failed
            if line['Instrument Check'] == 'FAIL' or line['Launch Status'] == 'MANUAL LAUNCH' and line['Top Up'] == 'NO':
                fail_metrics = sample_pse_match(compute_workflow, line['Full Name'], woid)
                fail_qc_update = dict(list(line.items())+list(fail_metrics.items()))

                #write passed samples to temp file, populate ready list
                if fail_metrics['Instrument Check'] == 'PASS' and fail_metrics['Launch Status'] == 'Launched':
                    temp_status_writer.writerow(fail_qc_update)
                    pse_fail_ready.append(fail_metrics['QC Sample'] + '\t' + fail_metrics['PSE'])

                #write failed samples to temp file, populate not ready list
                if fail_metrics['Instrument Check'] == 'FAIL':
                    temp_status_writer.writerow(fail_qc_update)
                    temp_launch_fail_writer.writerow(fail_qc_update)
                    pse_fail_fail.append(fail_metrics['QC Sample'] + "\t" + fail_metrics['PSE'] + "\t" +
                                         fail_metrics['# of Inputs'] + "\t" + fail_metrics['# of Instrument Data'])

                if (fail_metrics['Instrument Check'] == 'PASS') and (fail_metrics['Launch Status'] == 'MANUAL LAUNCH'):
                    temp_status_writer.writerow(fail_qc_update)
                    instrument_pass_status_active_writer.writerow(fail_qc_update)
                    ins_pass_stat_actv.append(fail_metrics['QC Sample'] + "\t" + fail_metrics['PSE'] + "\t" +
                                              fail_metrics['# of Inputs'] + "\t" + fail_metrics['# of Instrument Data']
                                              + '\t' + fail_metrics['LIMS Status'])

            #check sample status from sample email sent to be qc'd
            elif (line['Full Name'] in sample_list) and (line['Top Up'] == 'NO') and not line['QC Sample']:
                sample = line['Full Name']
                qc_metrics = sample_pse_match(compute_workflow, sample, woid)
                master_qc_update = dict(list(line.items())+list(qc_metrics.items()))

                #write passed samples to temp file
                if qc_metrics['Instrument Check'] == 'PASS' and qc_metrics['Launch Status'] == 'Launched':
                    temp_status_writer.writerow(master_qc_update)
                    pse_ready.append(qc_metrics['QC Sample'] + '\t' + qc_metrics['PSE'])

                #write failed samples to temp file
                if qc_metrics['Instrument Check'] == 'FAIL':
                    temp_status_writer.writerow(master_qc_update)
                    temp_launch_fail_writer.writerow(master_qc_update)
                    pse_fail.append(qc_metrics['QC Sample'] + "\t" + qc_metrics['PSE'] + "\t" + qc_metrics['# of Inputs']
                                    + "\t" + qc_metrics['# of Instrument Data'])

                if (qc_metrics['Instrument Check'] == 'PASS') and (qc_metrics['Launch Status'] == 'MANUAL LAUNCH'):
                    temp_status_writer.writerow(master_qc_update)
                    instrument_pass_status_active_writer.writerow(master_qc_update)
                    ins_pass_stat_actv.append(qc_metrics['QC Sample'] + "\t" + qc_metrics['PSE'] + "\t" +
                                              qc_metrics['# of Inputs'] + "\t" + qc_metrics['# of Instrument Data'] +
                                              '\t' + qc_metrics['LIMS Status'])

            #skip samples already checked to launch
            else:
                if line['Top Up'] == 'YES':
                    print('Skipping topup sample: {}'.format(line['Full Name']))
                elif line['Full Name'] in sample_list:
                    print('Ignoring previously launched sample: {}\t{}\t{}'
                          .format(line['Full Name'], line['Launch Date'], line['Launch Status']))

                #write all other unchecked samples to file
                temp_status_writer.writerow(line)

        #check to see if sample exists in status file
        for samp in sample_list:
            if not samp in qc_master_sample_list:
                print('{} not found in {}'.format(samp, qc_status_file))

    os.rename(temp_status_file, qc_status_file)
    os.rename(launch_failed_temp, launch_failed_file)
    os.rename(instrument_pass_status_active_file_tmp, instrument_pass_status_active_file)

    output_launcher_results(pse_ready, pse_fail, pse_fail_ready, pse_fail_fail, ins_pass_stat_actv)

    return

#Determine sample topup status
def topup_csv_update(topup_samples, woid):

    with open(woid + '.qcstatus.tsv', 'r') as qcstatuscsv, open(woid + '.qcstatus.temp.tsv', 'w') as tempstatuscsv:

        status_reader = csv.DictReader(qcstatuscsv, delimiter='\t')
        status_fieldnames = status_reader.fieldnames

        temp_status_writer = csv.DictWriter(tempstatuscsv, fieldnames=status_fieldnames, delimiter='\t')
        temp_status_writer.writeheader()

        for line in status_reader:
            if line['Full Name'] in topup_samples or line['Top Up'] == 'YES':
                line['Top Up'] = 'YES'
            else:
                line['Top Up'] = 'NO'
            temp_status_writer.writerow(line)

    os.rename(woid + '.qcstatus.temp.tsv',woid + '.qcstatus.tsv')

    return

#sample email function, create new woid dir if it doesn't exist and master spreadsheet if it doesn't exist.
#write emails to email file
def ccdg_launcher(infile):


    while True:

        sample_list = []

        woid = input('----------\nWork order id (enter to exit):\n').strip()
        # if not woid:
        if (len(woid) == 0):
            print('Exiting ccdg launcher.')
            break
        try:
            val = int(woid)
        except ValueError:
            print("\nwoid must be a number.")
            continue

        master_outfile = woid + '.master.samples.tsv'
        qc_status_file = woid + '.qcstatus.tsv'
        launch_failed_file = woid + '.launch.fail.tsv'

        #create woid dir if it does not exist
        if not os.path.exists(woid):
            print('\n{} dir does not exist.'.format(woid))
            create_woid = input('Create {} directory? (y or n)\n'.format(woid))
            if create_woid == 'y':
                os.makedirs(woid)
                os.chdir(woid)
                #create master file if it does not exist and also qcstatus file
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

                    with open(master_outfile, 'w') as mastercsv, open(qc_status_file, 'w') as statuscsv:
                        master_write = csv.writer(mastercsv, delimiter='\n')
                        status_write = csv.writer(statuscsv, delimiter='\n')
                        master_write.writerows([master_samples])
                        status_write.writerows([master_samples])

                    #Add header to qc status file and create failed to launch file with the same header.
                    with open(qc_status_file, 'r') as qcstempcsv, open('qc.status.temp.tsv', 'w') as qcstatusfilecsv, \
                            open(launch_failed_file, 'w') as launchfailedcsv:
                        temp_reader = csv.DictReader(qcstempcsv, delimiter = '\t')

                        temp_header = temp_reader.fieldnames
                        status_header = temp_header + qc_fieldnames

                        status_writer = csv.DictWriter(qcstatusfilecsv, fieldnames=status_header, delimiter='\t')
                        launch_fail_writer = csv.DictWriter(launchfailedcsv, fieldnames=status_header, delimiter='\t')
                        status_writer.writeheader()
                        launch_fail_writer.writeheader()

                        for line in temp_reader:
                            status_writer.writerow(line)

                        os.rename('qc.status.temp.tsv', qc_status_file)

                os.chdir(working_dir)

                if create_master == 'n':
                    pass
            if create_woid == 'n':
                continue

        #check for topup samples
        topup_samples = []
        tu_sample_y_n = input('\nTopup samples? (y or n)\n')
        if tu_sample_y_n == 'y':
            while True:
                tu_sample = input('Sample name: (enter to continue)\n' ).strip()
                if tu_sample:
                    if tu_sample[0] == '0':
                        tu_sample = tu_sample[1:]
                    topup_samples.append(tu_sample)
                else:
                    break

        #create sample email
        while True:
            sample_number = input('\nSample number:\n')
            try:
                val = int(sample_number)
            except ValueError:
                print('Sample number must be a number.')
            else:
                break

        user_date = input('\nUse today\'s date? (y or n)\n')
        if user_date == 'y':
            sample_outfile = sample_number + 'samples.' + mm_dd_yy
        elif user_date == 'n':
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

        #if there are topup add them to qcstatus file, populate with YES or NO
        topup_csv_update(topup_samples, woid)

        #write samples to email file
        with open(sample_outfile, 'w') as sample_outfilecsv:
            sample_write = csv.writer(sample_outfilecsv, delimiter='\n')
            sample_write.writerows([sample_info])

        #from sample file, use Library field to create sample list
        with open(sample_outfile, 'r') as samplecsv:

            sample_reader = csv.DictReader(samplecsv, delimiter='\t')

            for sample_line in sample_reader:
                sample_remove_whitespace = {k.replace(' ', ''): v for k, v in sample_line.items()}
                sample_name = sample_remove_whitespace['Library']
                sample_name_split = sample_name.split('-lib')[0]
                sample = sample_name_split.strip()
                if sample[0] == '0':
                    sample = sample[1:]
                sample_list.append(sample)

        #create compute workflow file
        woid_cw_file = compute_workflow_create(infile, woid)

        #check to see if samples exist in compute workflow file
        samples_not_found_in_cw = []
        for sample in sample_list:
            samples_not_found_in_cw = cw_sample_check(woid_cw_file, sample)

        if len(samples_not_found_in_cw) > 0:
            print('There are launch samples not found in {}:\n'.format(woid_cw_file))
            for sample in samples_not_found_in_cw:
                print(sample)
            quit()

        #run qc status updates on samples
        if os.path.exists(qc_status_file) and os.path.exists(woid_cw_file) and os.path.exists(launch_failed_file):
            qc_status_update(woid_cw_file, sample_list, woid, qc_status_file)

        os.chdir(working_dir)

    return

if __name__ == '__main__':
    main()

