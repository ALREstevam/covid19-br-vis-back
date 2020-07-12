from app import register_endpoints
import sys

if __name__ == '__main__':

    if len(sys.argv) == 2 and (sys.argv[1] == '-d' or sys.argv[1] == '--data-generate'):
        from app.DataGen import DataGen
        DataGen()\
            .download()\
            .register_default_steps()\
            .run()\
            .serialize_json_path()
    else:
        print("= RUNNING SERVER =")
        register_endpoints().run()


