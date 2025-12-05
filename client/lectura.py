from pymcprotocol import Type3E

plc = Type3E(plctype="Q")

IP = "192.168.3.39"
PORT = 9000

START_M = 1100
END_M = 1143
COUNT = END_M - START_M + 1  # 44 bits

try:
    plc.connect(IP, PORT)
    print("Conexión establecida con el PLC (MC Protocol)")

    bits = plc.batchread_bitunits(f"M{START_M}", COUNT)  # devuelve lista 0/1

    for i, v in enumerate(bits):
        print(f"m{START_M + i} = {int(v)}")

finally:
    plc.close()
