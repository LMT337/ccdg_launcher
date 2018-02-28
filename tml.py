import csv
import os
import datetime
import argparse
import webbrowser


#command line input parser
parser = argparse.ArgumentParser()

parser.add_argument("file", help='current workflow file from lims', type=str)
parser.add_argument("sample_qc", help='sample qc ready email', type=str)
parser.add_argument("-l", help='launch link in lims', action='store_true')
parser.add_argument("-m", help='master sample link in lims', action='store_true')
parser.add_argument("-s", help='email sample file cat', action='store_true')
parser.add_argument("-j", help='open jira issue', action='store_true')

args = parser.parse_args()

#get master file name from directory name
path = os.path.basename(os.getcwd())
master_file = path + '.master.samples.tsv'
outfile = path + '.master.samples.qcstatus.tsv'
do_not_launchcsv = path + '.inputs.instrument_data.check.fail.tsv'
do_not_launchcsv_temp = path + '.inputs.instrument_data.check.fail.tsv'

firefox_path = '/gapp/ia32linux/bin/firefox %s'
launch_link = "https://imp-lims.gsc.wustl.edu/entity/compute-workflow?setup_wo_id=" + path + "&protocol_id=761"

#generate date month/day/year string
date = datetime.datetime.now().strftime("%m-%d-%y")
date_time = datetime.datetime.now().strftime("%m%d%y")

if (args.j):
    open_url = "https://jira.gsc.wustl.edu/issues/?filter=12656&jql=project%20%3D%20QC%20AND%20statusCategory%20!%3D%20done%20and%20labels%20%3D%20TOPMed"
    webbrowser.get(firefox_path).open(open_url)

if (args.l):
    open_url = input("open launch url in firefox? (y/n)")
    if open_url == 'y':
        webbrowser.get(firefox_path).open(launch_link)
    else:
        print()
    print()
    print(launch_link)
    print()
    print('save workflow file as:')
    print(path + '.workflow.' + date_time + '.tsv\n')
    print('create workflow file with cat:\ncat > {}.workflow.{}.tsv\n'.format(path, date_time))

    quit()

if(args.m):

    master_sample_link = 'https://imp-lims.gsc.wustl.edu/entity/setup-work-order/' + path +'?perspective=Sample&setup_name=' + path

    open_url = input("open master url in firefox? (y/n)")
    if open_url == 'y':
        webbrowser.get(firefox_path).open(master_sample_link)
    else:
        print()

    print()
    print(master_sample_link)
    print()
    print('save master sample list as:')
    print(master_file)
    print('\ncreate master file with cat:\ncat > {}\n'.format(master_file))
    print("remove first line from " + master_file + " using:")
    print("sed -i '1d' " + master_file)
    print()
    quit()

if (args.s):
    sample_number = input('How many samples do you have?\n')
    date = input('Use today\'s date? (y or n)\n')
    if date == 'y':
        print('create sample file with:\ncat > {}samples.{}'.format(sample_number,date_time))
        quit()
    date = input('input date please (MMDDYY)\n')
    print('create sample file with:\ncat > {}samples.{}'.format(sample_number,date))
    quit()
	

qc_header = ['QC Sample', 'WOID', 'PSE', 'Instrument Check', 'Launch Status', 'Launch Date', '# of Inputs',
             '# of Instrument Data', 'QC Status', 'QC Date', 'QC Failed Metrics', 'COD Collaborator', 'QC Directory']


#remove -lib1 from email sample names, return list of correct names
def sample_name(argsamp):
    with open(argsamp) as sample_tsv:
        sample_reader = csv.DictReader(sample_tsv, delimiter="\t")
        sample_results = []
        for sample_email in sample_reader:
            # remove white space from dictionary
            sample_new = {k.replace(' ', ''): v for k, v in sample_email.items()}

            # remove -lib1 from sample name
            sample_name = sample_new['Library']
            sample_split = sample_name.split('-')[0]
            sample = sample_split.strip()
            sample_results.append(sample)

    return sample_results

#fix input sample name from qc ready email
email_sample_list = sample_name(args.sample_qc)

print('\n{} samples found in {} to prepare for launch:\n'.format(len(email_sample_list), args.sample_qc))
#Find sample matches in workflow tsv, return qc dictionary
def sample_pse_match(argfile, sample):
    qc_results = {}
    with open(argfile) as workfile_csv:
        workfile_reader = csv.DictReader(workfile_csv, delimiter="\t")
        for qc_data in workfile_reader:
            if sample == qc_data['Sample Full Name']:
                qc_results['QC Sample'] = qc_data['Sample Full Name']
                qc_results['PSE'] = qc_data['PSE']
                qc_results['# of Inputs'] = qc_data['# of Inputs']
                qc_results['# of Instrument Data'] = qc_data['# of Instrument Data']
                qc_results['Launch Date'] = date
                qc_results['WOID'] = path
                qc_results['QC Status'] = 'NONE'
                qc_results['QC Date'] = 'NONE'
                qc_results['QC Failed Metrics'] = 'NONE'
                qc_results['COD Collaborator'] = 'NONE'
                qc_results['QC Directory'] = 'NONE'

                if int(qc_data['# of Inputs']) == int(qc_data['# of Instrument Data']):
                    qc_results['Instrument Check'] = 'PASS'
                    qc_results['Launch Status'] = 'Launched'
                else:
                    qc_results['Instrument Check'] = 'FAIL'
                    qc_results['Launch Status'] = 'DO NOT LAUNCH'
                    qc_results['Launch Date'] = 'NA'


    return qc_results

qc_master_sample_list = []
if os.path.exists(outfile):

    temp_file = path + '.master.samples.partialqc.temp.tsv'
    with open(outfile, 'r') as outfilecsv, open(temp_file, 'w') as tempcsv, open(do_not_launchcsv, 'r') as dnlcsv\
            , open(do_not_launchcsv_temp, 'w') as dnlcsv_temp:

        reader = csv.DictReader(outfilecsv, delimiter="\t")

        headers = reader.fieldnames
        header_fields = headers

        dnl_temp = csv.DictWriter(dnlcsv_temp, header_fields, delimiter="\t")

        w = csv.DictWriter(tempcsv, header_fields, delimiter="\t")
        w.writeheader()

        pse_ready = []
        pse_fail = []

        pse_fail_ready = []
        pse_fail_fail = []

        for line in reader:

            qc_master_sample_list.append(line['Full Name'])

            if (line['Instrument Check'] == 'FAIL'):
                fail_metrics = sample_pse_match(args.file, line['Full Name'])
                fail_qc_update = dict(list(line.items()) + list(fail_metrics.items()))

                if fail_metrics['Instrument Check'] == 'PASS':
                    w.writerow(fail_qc_update)
                    pse_fail_ready.append(fail_metrics['PSE'])

                if fail_metrics['Instrument Check'] == 'FAIL':
                    w.writerow(fail_qc_update)
                    dnl_temp.writerow(fail_qc_update)
                    pse_fail_fail.append(fail_metrics['QC Sample'] + "\t" + fail_metrics['PSE'] + "\t" + fail_metrics['# of Inputs']
                                    + "\t" + fail_metrics['# of Instrument Data'])

            elif (line['Full Name'] in email_sample_list) and not line['QC Sample']:
                sample = line['Full Name']
                qc_metrics = sample_pse_match(args.file, sample)
                # combine master file and qc_metrics dictionaries, write to file
                master_qc_update = dict(list(line.items()) + list(qc_metrics.items()))

                if qc_metrics['Instrument Check'] == 'PASS':
                    w.writerow(master_qc_update)
                    pse_ready.append(qc_metrics['PSE'])

                if qc_metrics['Instrument Check'] == 'FAIL':
                    w.writerow(master_qc_update)
                    dnl_temp.writerow(master_qc_update)
                    pse_fail.append(qc_metrics['QC Sample'] + "\t" + qc_metrics['PSE'] + "\t" + qc_metrics['# of Inputs']
                                    + "\t" + qc_metrics['# of Instrument Data'])

            else:
                if line['Full Name'] in email_sample_list:
                    print('Ignoring previously analyzed sample: {}\t{}\t{}'
                          .format(line['Full Name'], line['Launch Date'], line['Launch Status']))
                w.writerow(line)

        for sample in email_sample_list:               
            if not sample in qc_master_sample_list:
                print('{} not found in {}'.format(sample, outfile))
        
        if len(pse_ready) > 0:
            print(str(len(pse_ready)) + " PSE ready for launch:")
            print(launch_link)
            for pse in pse_ready:
                print(pse)

            launch_confirm = input("Type 'y' to confirm build launch:" + "\n")
            if launch_confirm == 'y':
                print()
                print(outfile + " has been updated")
                print()
            else:
                print()
                print('No updates have been made to ' + outfile)

        if len(pse_fail) > 0:
            print(str(len(pse_fail)) + ' Samples Not Ready To Launch:')
            print('Sample', 'PSE', '# of Inputs', '# of Instrument Data', sep="\t")
            for pse in pse_fail:
                print(pse)
        else:
            print()
            print('All ' + args.sample_qc + ' samples are ready to launch or have a launch status\n')


        if len(pse_fail_ready) > 0:
            print('\n' + str(len(pse_fail_ready)) + ' ' + do_not_launchcsv +' samples now ready to launch:')
            print(launch_link)
            for pse in pse_fail_ready:
                print(pse)
            
            launch_confirm = input("Type 'y' to confirm build launch:" + "\n")
            if launch_confirm == 'y':
                print()
                print(outfile + " has been updated")
                print()

            else:
                print()
                print('No updates have been made to ' + outfile)


        if len(pse_fail_fail) > 0:
            print()
            print(do_not_launchcsv)
            print(str(len(pse_fail_fail)) + ' Samples waiting launch:')
            print('Sample', 'PSE', '# of Inputs', '# of Instrument Data', sep="\t")
            for pse in pse_fail_fail:
                print(pse)

    os.rename(temp_file, outfile)
    os.rename(do_not_launchcsv_temp, do_not_launchcsv)

else:

    with open(master_file) as master_tsv, open( outfile, 'w') as outfilecsv, open( do_not_launchcsv, 'w') as dnlcsv:

        #open file handles for input files
        reader = csv.DictReader(master_tsv, delimiter="\t")
        headers = reader.fieldnames
        header_fields = headers + qc_header

        #open filehandle for output file
        w = csv.DictWriter(outfilecsv, header_fields, delimiter="\t")
        w.writeheader()

        dnl = csv.DictWriter(dnlcsv, header_fields, delimiter="\t")
        dnl.writeheader()

        pse_ready = []
        pse_fail = []

        for line in reader:
            
            qc_master_sample_list.append(line['Full Name'])            
            if line['Full Name'] in email_sample_list:
                sample = line['Full Name']
                qc_metrics = sample_pse_match(args.file, sample)
                # combine master file and qc_metrics dictionaries, write to file
                master_qc_update = dict(list(line.items()) + list(qc_metrics.items()))

                if qc_metrics['Instrument Check'] == 'PASS':
                    w.writerow(master_qc_update)
                    pse_ready.append(qc_metrics['PSE'])

                if qc_metrics['Instrument Check'] == 'FAIL':
                    w.writerow(master_qc_update)
                    dnl.writerow(master_qc_update)
                    pse_fail.append(qc_metrics['QC Sample'] + "\t" + qc_metrics['PSE'] + "\t" + qc_metrics['# of Inputs']
                                    + "\t" + qc_metrics['# of Instrument Data'])

            else:
                w.writerow(line)
        
        for sample in email_sample_list:               
            if not sample in qc_master_sample_list:
                print('{} not found in {}'.format(sample, outfile))

        if len(pse_ready) > 0:
            print('\n' + str(len(pse_ready)) + " PSE's ready for launch:")
            print(launch_link)
            for pse in pse_ready:
                print(pse)

            launch_confirm = input("Type 'y' to confirm build launch:" + "\n")
            if launch_confirm == 'y':
                print()
                print(outfile + ' has been created' )
                print()
            else:
                print()
                print(outfile + ' and ' + do_not_launchcsv +' have not been created')
                os.remove(outfile)
                os.remove(do_not_launchcsv)
                print("exiting tml.py")
                quit()


        if len(pse_fail) > 0:
            print(str(len(pse_fail)) + ' Samples not ready to launch:')
            print('Sample', 'PSE', '# of Inputs', '# of Instrument Data', sep="\t")
            for pse in pse_fail:
                print(pse)
        else:
            print('All ' + args.sample_qc + ' samples are ready to launch or have a launch status\n')

quit()

