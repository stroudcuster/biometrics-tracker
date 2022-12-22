import collections
from datetime import date, time
from io import IOBase
from fpdf import FPDF
import queue
import threading
from typing import Optional, Callable

import biometrics_tracker.ipc.messages as messages
import biometrics_tracker.ipc.queue_manager as queue_mgr
import biometrics_tracker.model.datapoints as dp
import biometrics_tracker.model.persistence as per
from biometrics_tracker.utilities import utilities as utilities


class ReportBase(threading.Thread):
    """
    A base class for classes that produce reports from the Biometrics Tracking database
    """
    def __init__(self, queue_mgr: queue_mgr.Queues, person: dp.Person, start_date: date, end_date: date):
        """
        Create and instance of model.reports.ReportBase

        :param queue_mgr: an object containing the queues used for inter-thread communication
        :type queue_mgr: biometrics_tracker.ipc.queue_manager.Queues
        :param person: info for the person whose data will be reported
        :type person: biometrics_tracker.model.datapoints.Person
        :param start_date: start of datapoint date range
        :type start_date: datetime.date
        :param end_date: end of datapoint date range
        :type end_date: datetime.date

        """
        threading.Thread.__init__(self)
        self.person = person
        self.queue_mgr = queue_mgr
        self.start_date: Optional[date] = start_date
        self.end_date: Optional[date] = end_date
        self.col_dp_map: dict[dp.DataPointType, int] = {}
        for idx, track_cfg in enumerate(self.person.tracked.values()):
            self.col_dp_map[track_cfg.dp_type] = idx + 2
        self.replyto: Optional[Callable] = None

    def run(self) -> None:
        start_datetime = utilities.mk_datetime(self.start_date, time(hour=0, minute=0, second=0))
        end_datetime = utilities.mk_datetime(self.end_date, time(hour=23, minute=59, second=59))
        self.queue_mgr.send_db_req_msg(messages.DataPointReqMsg(destination=per.DataBase, replyto=self.replyto,
                                                                person_id=self.person.id, start=start_datetime,
                                                                end=end_datetime,
                                                                operation=messages.DBOperation.RETRIEVE_SET))
        resp: messages.DataPointRespMsg = self.queue_mgr.check_db_resp_queue(block=True)
        if resp is not None and resp.destination is not None:
            resp.destination(resp)


class TextReport(ReportBase):
    def __init__(self, queue_mgr: queue_mgr.Queues, person: dp.Person, start_date: date, end_date: date, outf: IOBase,
                 completion_destination: Callable):
        """
        Create and instance of model.reports.ReportBase

        :param queue_mgr: an object containing the queues used for inter-thread communication
        :type queue_mgr: biometrics_tracker.ipc.queue_manager.Queues
        :param person: info for the person whose data will be reported
        :type person: biometrics_tracker.model.datapoints.Person
        :param start_date: start of datapoint date range
        :type start_date: datetime.date
        :param end_date: end of datapoint date range
        :type end_date: datetime.date
        :param outf: output connection
        :type outf: descendant of io.IOBase
        :param completion_destination: the method to be used for the destination of the completion message
        :type completion_destination: Callable

        """
        ReportBase.__init__(self, queue_mgr, person, start_date, end_date)
        self.outf = outf
        self.completion_destination = completion_destination
        self.replyto = self.produce_report
        pass

    def produce_report(self, msg: messages.DataPointRespMsg):
        """
        Produce a report listing datapoints for a specified person and date range, then write it to a text file

        :param msg: a message object containing the list of datapoints to be included in the report
        :return: None

        """

        def build_line(cells: collections.OrderedDict) -> list[str]:
            line: list[str] = [last_date.strftime('%m/%d/%Y').ljust(15),
                               last_time.strftime('%I:%M %p').ljust(15)]
            for x in range(0, len(self.col_dp_map.values())):
                line.append(' '.ljust(18))
            # metric is a tuple containing a metric instance (e.g. BodyWeight, BloodPress, etc) and optionally
            # a note citation
            for col_idx, metric in cells.items():
                if metric[1] > 0:
                    if metric[1] < 10:
                        line[col_idx] = f'{metric[0].__str__()} ({metric[1]:d})'.rjust(18)
                    elif metric[1] < 100:
                        line[col_idx] = f'{metric[0].__str__()} ({metric[1]:2d})'.rjust(18)
                    else:
                        line[col_idx] = f'{metric[0].__str__()} ({metric[1]:3d})'.rjust(18)
                else:
                    line[col_idx] = metric[0].__str__().rjust(18)
            return line

        def print_line(line: list[str]) -> None:
            for col in line:
                print(col, end='', file=self.outf)
            print(' ', file=self.outf)

        datapoints: list[dp.dptypes_union] = msg.datapoints
        last_date: Optional[date] = None
        last_time: Optional[time] = None
        rpt_cells = collections.OrderedDict()
        print('Biometrics History Report\n'.rjust(50), file=self.outf)
        print(f'Person ID: {self.person.id}   Name: {self.person.name}   Date of Birth: '
              f'{self.person.dob.strftime("%m/%d/%Y")}\n', file=self.outf)
        rpt_hdg: list[str] = ['Date'.ljust(15), 'Time:'.ljust(15)]
        for x in range(0, len(self.col_dp_map.values())):
            rpt_hdg.append(' '.rjust(18))
        for dp_type, idx in self.col_dp_map.items():
            rpt_hdg[idx] = dp.dptype_dp_map[dp_type].label().rjust(18)
        print_line(rpt_hdg)
        page_size = 30
        row_idx: int = 1
        note_nbr: int = 1
        note_list: list[str] = []
        for datapoint in datapoints:
            dp_date, dp_time = utilities.split_datetime(datapoint.taken)
            if last_date is not None and (dp_date != last_date or dp_time != last_time):
                rpt_line = build_line(rpt_cells)
                if row_idx > page_size:
                    print_line(rpt_hdg)
                    row_idx = 1
                print_line(rpt_line)
                rpt_cells = collections.OrderedDict()
                row_idx += 1
            last_date = dp_date
            last_time = dp_time
            rpt_cell = None
            if len(datapoint.note.strip()) > 0:
                try:
                    note_idx = note_list.index(datapoint.note)
                    rpt_cell = (datapoint.data, note_idx + 1)
                except ValueError:
                    note_list.append(datapoint.note)
                    rpt_cell = (datapoint.data, note_nbr)
                    note_nbr += 1
            else:
                rpt_cell = (datapoint.data, 0)
            rpt_cells[self.col_dp_map[datapoint.type]] = rpt_cell
        if last_date is not None:
            rpt_line = build_line(rpt_cells)
            if row_idx > page_size:
                print_line(rpt_hdg)
                row_idx = 1
            print_line(rpt_line)
        if len(note_list) > 0:
            print('Notes:\n', file=self.outf)
            for note_nbr, note in enumerate(note_list):
                print(f'{note_nbr+1:3d}: {note}\n', file=self.outf)
        msg = messages.CompletionMsg(destination=self.completion_destination,
                                     replyto=None, status=messages.Completion.SUCCESS)
        self.queue_mgr.send_completion_msg(msg)


class PDFReport(ReportBase):
    def __init__(self, queue_mgr: queue_mgr.Queues, person: dp.Person, start_date: date, end_date: date,
                 rpt_filename: str, completion_destination: Callable):
        """
        Create and instance of model.reports.PDFReport

        :param queue_mgr: an object containing the queues used for inter-thread communication
        :type queue_mgr: biometrics_tracker.ipc.queue_manager.Queues
        :param person: info for the person whose data will be reported
        :type person: biometrics_tracker.model.datapoints.Person
        :param start_date: start of datapoint date range
        :type start_date: datetime.date
        :param end_date: end of datapoint date range
        :type end_date: datetime.date
        :param rpt_filename: the file path the report is to be written to
        :type rpt_filename: str
        :param completion_destination: a method to be invoked when the report process has complete
        :type completion_destination: Callable


        """
        ReportBase.__init__(self, queue_mgr, person, start_date, end_date)
        self.rpt_filename = rpt_filename
        self.completion_destination = completion_destination
        self.replyto = self.produce_report

    def produce_report(self, msg: messages.DataPointRespMsg):
        """
        Retrieve the datapoints within the specified date range for the selected person and produce a pdf report

        :return: None

        """
        def build_line() -> None:
            rpt_line: list[str] = [last_date.strftime('%m/%d/%Y').rjust(11),
                                   last_time.strftime('%I:%M %p').rjust(9)]
            for x in range(0, len(self.col_dp_map.values())):
                rpt_line.append(' '.rjust(18))
            # metric is a tuple containing a metric instance (e.g. BodyWeight, BloodPress, etc) and optionally
            # a note citation
            for col_idx, metric in rpt_cells.items():
                if metric[1] > 0:
                    if metric[1] < 10:
                        rpt_line[col_idx] = f'{metric[0].__str__()} ({metric[1]:d})'.rjust(18)
                    elif metric[1] < 100:
                        rpt_line[col_idx] = f'{metric[0].__str__()} ({metric[1]:2d})'.rjust(18)
                    else:
                        rpt_line[col_idx] = f'{metric[0].__str__()} ({metric[1]:3d})'.rjust(18)
                else:
                    rpt_line[col_idx] = metric[0].__str__().rjust(18)
            row_idx = 1
            for idx, col in enumerate(rpt_line):
                if idx == len(rpt_line) - 1:
                    pdf.cell(pdf.get_string_width(col), line_height, txt=col, new_x='LMARGIN', new_y='NEXT', align='R')
                else:
                    pdf.cell(pdf.get_string_width(col), line_height, txt=col, align='R')

        line_height = 9
        datapoints: list[dp.dptypes_union] = msg.datapoints

        last_date: Optional[date] = None
        last_time: Optional[time] = None
        rpt_cells = collections.OrderedDict()
        page_heading: list[str] = ['Biometrics History Report'.rjust(50),
                                   f'Person ID: {self.person.id}   Name: {self.person.name}'
                                   f'  Date of Birth: {self.person.dob.strftime("%m/%d/%Y")}']
        col_heading: list[str] = ['Date'.rjust(11), 'Time'.rjust(9)]
        for x in range(0, len(self.col_dp_map.values())):
            col_heading.append(' '.rjust(18))
        for dp_type, idx in self.col_dp_map.items():
            col_heading[idx] = dp.dptype_dp_map[dp_type].label().rjust(18)

        pdf = BiometricsHistoryPDF(page_heading, [col_heading], line_height)

        note_nbr: int = 1
        note_list: list[str] = []
        pdf.set_font('Courier', '', 12)
        pdf.add_page(orientation='L')
        for datapoint in datapoints:
            dp_date, dp_time = utilities.split_datetime(datapoint.taken)
            if last_date is not None and (dp_date != last_date or dp_time != last_time):
                build_line()
                rpt_cells = collections.OrderedDict()
            last_date = dp_date
            last_time = dp_time
            rpt_cell = None
            if len(datapoint.note.strip()) > 0:
                try:
                    note_idx = note_list.index(datapoint.note)
                    rpt_cell = (datapoint.data, note_idx + 1)
                except ValueError:
                    note_list.append(datapoint.note)
                    rpt_cell = (datapoint.data, note_nbr)
                    note_nbr += 1
            else:
                rpt_cell = (datapoint.data, 0)
            rpt_cells[self.col_dp_map[datapoint.type]] = rpt_cell
        if last_date is not None:
            build_line()
        if len(note_list) > 0:
            pdf.cell(0, line_height, txt='Notes:', new_x='LMARGIN', new_y='NEXT', align='L')
            for note_nbr, note in enumerate(note_list):
                pdf.cell(0, line_height, txt=f'{note_nbr+1:3d}: {note}', new_x='LMARGIN', new_y='NEXT')
        pdf.output(self.rpt_filename)
        self.queue_mgr.send_completion_msg(messages.CompletionMsg(destination=self.completion_destination,
                                                                  replyto=None,
                                                                  status=messages.Completion.SUCCESS))


class BiometricsHistoryPDF(FPDF):
    """
    Provides page header and footer functionality for the PDF version of the Biometrics History Report
    """
    def __init__(self, page_heading: list[str], col_heading: list[list[str]], line_height):
        """
        Creates an instance of BiometricsHistoryPDF

        :param page_heading: a list of stings containing the text of the page heading for the report
        :type page_heading: list[str]
        :param col_heading: a list of lists of strings containing the column headings for the report
        :type col_heading list[list[str]]
        :param line_height: the line height to be used when creating cells
        :type line_height: int

        """
        FPDF.__init__(self)
        self.page_heading: list[str] = page_heading
        self.col_heading: list[list[str]] = col_heading
        self.line_height = line_height

    def header(self):
        """
        Produce a page header.  Overrides the header method in FPDF

        :return: None
        """
        for line in self.page_heading:
            width = self.get_string_width(line) + 6
            self.cell(width, self.line_height, txt=line, new_x='LMARGIN', new_y='NEXT', align='L')
        for line in self.col_heading:
            for col in line:
                self.cell(self.get_string_width(col), self.line_height, txt=col, align='R')
        self.ln()

    def footer(self):
        """
        Produce a page footer.  Overrides the footer method in FPDF

        :return: None
        """
        # Setting position at 1.5 cm from bottom:
        self.set_y(-15)
        # Setting font: helvetica italic 8
        self.cell(0, 10, f'Page {self.page_no()}', align="C")
