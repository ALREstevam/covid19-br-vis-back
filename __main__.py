from app.api import flask_api

if __name__ == "__main__":

    import os
    from os.path import join, dirname
    from dotenv import load_dotenv
    from waitress import serve
    import os
    

    def get_env_vars():
        dotenv_path = join(dirname(__file__), '.env')
        load_dotenv(dotenv_path)
        return os.environ['PORT'] or os.getenv('PORT'),\
            os.environ['RUN_MODE'] or os.getenv('RUN_MODE'),\
            os.environ['HOST'] or os.getenv('HOST')
    
    def display_env_vars():
        port, run_mode, host = get_env_vars()
        print('ENVIRONMENT VARIABLES')
        print(f'HOST=`{host}`')
        print(f'PORT=`{port}`')
        print(f'RUN_MODE=`{run_mode}`')

    display_env_vars()
    port, run_mode, host = get_env_vars()

    if run_mode == 'PROD':
        serve(flask_api, host=host or 'localhost', port=port or 5000)
    else:
        flask_api.run(host=host or 'localhost', port=port or 5000)
    