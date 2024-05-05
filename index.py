from logger import Logger
from pantheon_controller import PantheonController

selection = None
pantheon_controller = PantheonController()

while selection != 0:
    Logger.warn('Which function would you like to use?')
    Logger.warn('1.) Load procedures')
    Logger.warn('2.) Load tables')
    Logger.warn('3.) Analyze procedure references')
    Logger.warn('4.) Search for table references in a procedure')
    Logger.warn('5.) Clone database')
    Logger.warn('0.) Exit')
    selection = int(Logger.input('Selection: '))

    match selection:
        case 1:
            pantheon_controller.load_procedures()
        case 2:
            pantheon_controller.load_tables()
        case 3:
            pantheon_controller.load_procedure_calls()
        case 4:
            procedure_name = Logger.input('Procedure name: ')
            table_name = Logger.input('Table name: ')
            pantheon_controller.find_table_in_procedure(procedure_name, table_name)
        case 5:
            pantheon_controller.clone_database()
        case 0:
            exit()