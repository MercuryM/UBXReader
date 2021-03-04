from sys import platform, exit
from io import BufferedReader
from threading import Thread
import time
from datetime import datetime, timedelta
from pyubx2 import UBXMessage, POLL, UBX_MSGIDS
from pyubx2.ubxreader import UBXReader
from serial import Serial, SerialException, SerialTimeoutException

import pyubx2.exceptions as ube
import pandas as pd

class UBXStreamer:
    """
    UBXStreamer class.
    """

    def __init__(self, port, baudrate, timeout=5):
        """
        Constructor.
        """

        self._serial_object = None
        self._serial_thread = None
        self._ubxreader = None
        self._connected = False
        self._reading = False
        self._port = port
        self._baudrate = baudrate
        self._timeout = timeout

    def __del__(self):
        """
        Destructor.
        """

        self.stop_read_thread()
        self.disconnect()

    def connect(self):
        """
        Open serial connection.
        """

        try:
            self._serial_object = Serial(
                self._port, self._baudrate, timeout=self._timeout
            )
            self._ubxreader = UBXReader(BufferedReader(self._serial_object), False)
            self._connected = True
        except (SerialException, SerialTimeoutException) as err:
            print(f"Error connecting to serial port {err}")

    def disconnect(self):
        """
        Close serial connection.
        """

        if self._connected and self._serial_object:
            try:
                self._serial_object.close()
            except (SerialException, SerialTimeoutException) as err:
                print(f"Error disconnecting from serial port {err}")
        self._connected = False

    def start_read_thread(self):
        """
        Start the serial reader thread.
        """

        if self._connected:
            self._reading = True
            self._serial_thread = Thread(target=self._read_thread, daemon=True)
            self._serial_thread.start()

    def stop_read_thread(self):
        """
        Stop the serial reader thread.
        """

        if self._serial_thread is not None:
            self._reading = False

    def flush(self):
        """
        Flush input buffer
        """

        self._serial_object.reset_input_buffer()

    def waiting(self):
        """
        Check if any messages remaining in the input buffer
        """

        return self._serial_object.in_waiting

    def _read_thread(self):
        """
        THREADED PROCESS
        Reads and parses UBX message data from stream
        """
        start_time = time.time()
        header = ['TS', 'time', 'lat', 'lon', 'speed', 'fix']
        global gnss_points_df
        gnss_points_df = pd.DataFrame(columns=header)

        while self._reading and self._serial_object:
            if self._serial_object.in_waiting:
                start_time = time.time()
                try:
                    (raw_data, parsed_data) = self._ubxreader.read()
                    #                     if raw_data:
                    #                         print(raw_data)
                    if parsed_data:
                        print(parsed_data)
                        print(parsed_data.identity)
                        if parsed_data.identity == 'NAV-PVT':
                            utc_time = datetime(1980, 1, 6) + timedelta(seconds=(parsed_data.iTOW / 1000) - (35 - 19))
                            utc_time = utc_time.replace(year = parsed_data.year, month = parsed_data.month, day = parsed_data.day)
                            ts = time.mktime(utc_time.timetuple()) * 1000.0 + utc_time.microsecond / 1000.0

                            if parsed_data.fixType == 3:
                                fix = "3d"
                            elif parsed_data.fixType == 2:
                                fix = "2d"
                            else:
                                fix = "none"
                            loc_str = [ts, utc_time.strftime('%Y-%m-%d %H:%M:%S.%f '), parsed_data.lat/10**7, parsed_data.lon/10**7, parsed_data.gSpeed, fix]
                            print(loc_str)
                            gnss_points_df.loc[ts] = loc_str

                except (
                    ube.UBXStreamError,
                    ube.UBXMessageError,
                    ube.UBXTypeError,
                    ube.UBXParseError,
                ) as err:
                    print(f"Something went wrong {err}")
                    continue
            else:
                count_time = time.time()-start_time
                if count_time > self._timeout:
                    print("\n\nStopping reader thread...")
                    self.stop_read_thread()
                    print("Disconnecting from serial port...")
                    self.disconnect()
                    break


if __name__ == "__main__":

    # set PORT, BAUDRATE and TIMEOUT as appropriate
    if platform == "win32":
        PORT = "COM5"
    else:
        PORT = "/dev/ttyACM0"
    BAUDRATE = 9600
    TIMEOUT = 5
    # NMEA = 0
    # UBX = 1
    # BOTH = 2

    filename = 'GPS.csv'
    dt = datetime.now()
    nowtime_str = dt.strftime('%y-%m-%d %I-%M-%S')  # 时间
    filename = nowtime_str + '_' + filename
    header = ['TS', 'time', 'lat', 'lon', 'speed', 'fix']

    print("Instantiating UBXStreamer class...")
    ubp = UBXStreamer(PORT, BAUDRATE, TIMEOUT)
    print(f"Connecting to serial port {PORT} at {BAUDRATE} baud...")
    ubp.connect()
    print("Starting reader thread...")
    ubp.start_read_thread()
    time.sleep(6)
    flag = len(gnss_points_df)
    pd.DataFrame(gnss_points_df).to_csv(filename, header=header, index=False)
    while True:
        time.sleep(6)
        if len(gnss_points_df) == flag:
            break
        else:
            print('writing...')
            pd.DataFrame(gnss_points_df.iloc[flag - 1: -1]).to_csv(filename, mode='a+', header = False, index = False)
            flag = len(gnss_points_df)

    exit()
