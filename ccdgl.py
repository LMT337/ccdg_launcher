import argparse, csv, os, webbrowser, datetime

#generate date month/day/year string
mm_dd_yy = datetime.datetime.now().strftime("%m%d%y")

def main():
    desc_str = """
        Program to create sample emails and update sample launch status in tracking spreadsheets.
    """

    parser = argparse.ArgumentParser(description=desc_str)

    group = parser.add_mutually_exclusive_group()

    group.add_argument('-file', nargs=1, dest='infile', type=str, help='Input compute workflow file with all samples')
    group.add_argument('-l', help='Link to links compute workflow file', action='store_true')

    args = parser.parse_args()

    if args.l:
        generate_compute_workflow()

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


# def sample_email:


if __name__ == '__main__':
    main()

