from pymcprotocol import Type3E
import time

def main():
    # Configuración básica
    PLC_IP = "192.168.3.39"
    PLC_PORT = 9000
    
    # Inicializar el cliente para PLC serie Q (Type 3E)
    plc = Type3E(plctype="Q")
    
    try:
        print(f"Intentando conectar a {PLC_IP}:{PLC_PORT}...")
        plc.connect(PLC_IP, PLC_PORT)
        print("¡Conectado con éxito!")

        while True:
            # 1. Leer 10 bits desde M410
            bits = plc.batchread_bitunits("M410", 10)
            
            # 2. Leer 5 registros (words) desde D100
            words = plc.batchread_wordunits("D100", 5)
            
            print(f"--- Lectura {time.strftime('%H:%M:%S')} ---")
            print(f"Bits (M410-M419): {bits}")
            print(f"Valores (D100-D104): {words}")
            
            # Esperar 2 segundos antes de la siguiente lectura
            time.sleep(2)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        plc.close()
        print("Conexión cerrada.")

if __name__ == "__main__":
    main()
