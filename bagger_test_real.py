import subprocess
import datetime
from time import sleep
from pyModbusTCP.client import ModbusClient
# Please refer to phoenix_io_test.py, you can copy the class from there
bag2pc_bitmap={
    0 : (8000, 8),
    1 : (8000, 9),
    2 : (8000, 10),
    3 : (8000, 11),
    10 : (8000, 12),
    11 : (8000, 13),
    12 : (8000, 14),
    13 : (8000, 15),
    20 : (8000, 0),
    21 : (8000, 1),
    22 : (8000, 2),
    23 : (8000, 3),
    30 : (8000, 4),
    31 : (8000, 5),
    32 : (8000, 6),
    33 : (8000, 7),
    40 : (8001, 8),
    41 : (8001, 9),
    42 : (8001, 10),
    43 : (8001, 11),
    50 : (8001, 12),
    51 : (8001, 13),
    52 : (8001, 14),
    53 : (8001, 15),
    60 : (8001, 0),
    61 : (8001, 1),
    62 : (8001, 2),
    63 : (8001, 3),
    70 : (8001, 4),
    71 : (8001, 5),
    72 : (8001, 6),
    73 : (8001, 7),
}
pc2bag_bitmap={
    0:(9002, 8),
    1:(9002, 9),
    2:(9002, 10),
    3:(9002, 11),
    10:(9002, 12),
    11:(9002, 13),
    12:(9002, 14),
    13:(9002, 15),
    20:(9002, 0),
    21:(9002, 1),
    22:(9002, 2),
    23:(9002, 3),
    30:(9002, 4),
    31:(9002, 5),
    32:(9002, 6),
    33:(9002, 7),
    40:(9003, 8),
    41:(9003, 9),
    42:(9003, 10),
    43:(9003, 11),
    50:(9003, 12),
    51:(9003, 13),
    52:(9003, 14),
    53:(9003, 15),
    60:(9003, 0),
    61:(9003, 1),
    62:(9003, 2),
    63:(9003, 3),
    70:(9003, 4),
    71:(9003, 5),
    72:(9003, 6),
    73:(9003, 7),
}
class Induction:
    servo_on = 0 #Coupler output 02 --> DI 1 (B6)
    alarm_reset = 0 #Coupler output 03 --> DI 4 (A8)

    start = 0 #Coupler output 12 --> DI 5 (B8)
    stop = 0 #Coupler output 13 -->  DI 6 (A9)
    idx0 = 0 #Coupler output 22 --> DI 11 (B12)
    idx1 = 0 #Coupler output 23 --> DI 12 (A13)
    num_IO = 6 #number of IO pins used to controller motor (number of variables above)
    DO_register = 9000 #Register for writing digital outs
    DO_value = 0 #value we will write to register
    def __init__(self, host = '192.168.3.10'):
        self.host = host #host of IP address
        #create and connect to IO Coupler
        # connect to the client and activate the connection
        self.client = ModbusClient(self.host)
        self.client.open()
        print("connected!!")
        self.bit1 = [0]*16 # for 9002
        self.bit2 = [0]*16 # for 9003
    #Reset all IO outputs to low
    def get_bagger_error_number(self):
        pin_numbers=[50, 51, 52, 53, 60, 61, 62]
        bits=[self.read_bit_io(*bag2pc_bitmap[pin]) for pin in pin_numbers[::-1]]
        return int(''.join('1' if g else '0' for g in bits),2)

    def reset_io(self):
        self.servo_on = 0
        self.alarm_reset = 0
        self.start = 0
        self.stop = 0
        self.set_position('h')
        self.update_value()
        self.client.write_single_register(self.DO_register, self.DO_value)
    #Set internal IDX variables to match posiiton requested
    def set_position(self, pos = 'h'):
        if pos == 'h':
            #Corresponds to index 0 in DriveCM software
            self.idx0 = 0
            self.idx1 = 0
        elif pos == "i":
            #Corresponds to index 1 in DriveCM software
            self.idx0 = 1
            self.idx1 = 0
        elif pos == 'q':
            #Corresponds to index 2 in DriveCM software
            self.idx0 = 0
            self.idx1 = 1
        elif pos == 'fix':
            #Corresponds to index 3 in DriveCM software
            self.idx0 = 1
            self.idx1 = 1
        else:
            print("Position not recognized. Options are 'h' for home, 'i' for induct, and 'q' for QA")
    #convert bits to register number
    def update_value(self):
        return int(str(self.idx1)+str(self.idx0)+str(self.stop)+str(self.start)+str(self.alarm_reset)+str(self.servo_on), 2)
    def update_value_bit(self):
        self.bit1[4] = self.servo_on
        self.bit1[5] = self.alarm_reset
        self.bit1[6] = self.start
        self.bit1[7] = self.stop
        self.bit2[8] = self.idx0
        self.bit2[9] = self.idx1
    def write_ana_io(self, pin, value):
        #res = self.client.write_single_register(pin, value)
        res = self.client.write_multiple_registers(pin, [value, value])
        print(res)
    def write_io(self, addr, value):
        res = self.client.write_single_register(addr, value)
        print("result: ", res)
        print("last error: ", self.client.last_error_as_txt)
        print("last txt: ", self.client.last_except_as_full_txt)
    def read_io(self, pin, value):
        res = [int(y, 16) for y in [hex(x) for x in self.client.read_input_registers(pin, value)]]
        return res
    def read_bit_io(self, pin, value):
        register = self.client.read_input_registers(pin, 1)[0]
        return (register & (1 << value) > 0)
    #Move to whatever current output register is
    def move(self, pos, addr1, addr2):
        #keep servo on but deactivate start
        # self.servo_on = 1
        # self.start = 0
        # self.set_position(pos) #update position registers
        # do_value = self.update_value()
        # self.client.write_single_register(addr, do_value)#write it to the coupler
        # sleep(0.1) #slight delay before start
        # self.start = 1 #engage start bit
        # do_value = self.update_value()
        # self.client.write_single_register(addr, do_value)#write it to the coupler
        self.servo_on = 1
        self.start = 0
        self.set_position(pos)
        self.update_value_bit()
        logger.info("start is 0..")
        logger.info(self.bit1)
        logger.info(self.bit2)
        val1 = 0
        val2 = 0
        for i in range(len(self.bit2)):
            val1 += self.bit1[i] * 2 ** i
            val2 += self.bit2[i] * 2 ** i
        self.client.write_single_register(reg_addr=addr1, reg_value=val1)
        self.client.write_single_register(reg_addr=addr2, reg_value=val2)
        sleep(0.1)
        self.start = 1
        self.update_value_bit()
        val1 = 0
        val2 = 0
        logger.info("start is 1..")
        logger.info(self.bit1)
        logger.info(self.bit2)
        for i in range(len(self.bit2)):
            val1 += self.bit1[i] * 2 ** i
            val2 += self.bit2[i] * 2 ** i
        self.client.write_single_register(reg_addr=addr1, reg_value=val1) #+2**0)
        self.client.write_single_register(reg_addr=addr2, reg_value=val2)

    def open_bag(self):
        ### Open the bag
        # command to send label to printer via tcp, address and port are define in pnp/config.yaml
        command = "printf '\\eA\\eV1420\\eH358\\eD2052701234567890123\\eQ1\\eZ' | nc -w3 111.1.1.22 9100"
        # Execute the command, this line will print and open bag
        result = subprocess.run(command, shell=True, capture_output=True, text=True)

    def close_bag(test):
        ### Close the bag
        test.write_io(addr=9002, value=(2**12+2**13)) # high
        sleep(1)
        test.write_io(addr=9002, value=(2**12)) # low, then sealing the bag

    def wait_for_bagger_ready(self):
        start_time=datetime.datetime.now()
        while not self.read_bit_io(*bag2pc_bitmap[40]):
            sleep(0.1)
            if datetime.datetime.now()-start_time>datetime.timedelta(seconds=60):
                return False
        return True
    def repeat_open_close(self,base=0):
        # target = 100
        target = int(input("target num?"))
        while base<target:
            sleep(3)
            is_ready_after_all=self.wait_for_bagger_ready()
            if not is_ready_after_all: 
                print('timeout. ')
                return base
            is_ready_after_all=self.wait_for_bagger_ready()
            self.open_bag()
            sleep(0.75)
            error_code=self.get_bagger_error_number()
            if error_code == 0:
                self.close_bag()
                base=base+1
                print(base)
            else:
                print(f'‼️  BAG FAIL {error_code} !!')
                return base
        return base
induction=Induction()

induction.repeat_open_close()
