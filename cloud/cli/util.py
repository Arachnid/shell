import logging

class NullHandler(logging.Handler):
    """Does nothing with log messages. Included by default in the Python
    library for version 2.7+, but necessary for 2.6 compatibility"""
    
    def emit(self, record):
        pass

def list_of_dicts_printer(headers):
    """Prints a list of dictionaries as a table with an optional header"""
    
    def list_of_dicts_printer_helper(l, print_headers=True):
        
        spacing = {}
        for header in headers:
            max_column_width = len(header) if print_headers else 0
            for item in l:
                max_column_width = max(max_column_width, len(str(item.get(header, ''))))
            spacing[header] = max_column_width
        
        if print_headers:
            for header in headers:
                print '%s%s'  % (header, ' ' * (spacing[header] - len(header) + 4)),
            print ''
        
        for item in l:
            for header in headers:
                print '%s%s'  % (item.get(header, ''), ' ' * (spacing[header] - len(str(item.get(header, ''))) + 4)),
            print ''
    
    return list_of_dicts_printer_helper

def dict_printer(headers):
    """Prints a dictionary as a table with one row with an optional header"""
    
    def dict_printer_helper(d, print_headers=True):
        list_of_dicts_printer(headers)([d], print_headers)
    
    return dict_printer_helper

def list_printer(header):
    """Prints a list with one element per line with an optional header"""
    
    def list_printer_helper(l, print_headers=True):
        
        if print_headers:
            print header
        
        for item in l:
            print item
        
    return list_printer_helper

def volume_ls_printer(listings, _):
    """Prints the result of volume ls."""
    entries_printer = list_of_dicts_printer(['name', 'size', 'modified'])
    num_paths = len(listings)
    for i in range(num_paths):
        path, listing = listings[i]
        print path
        print 'total %s' % len(listing)
        entries_printer(listing, print_headers=False)
        if i < (num_paths - 1):
            print


from _abcoll import KeysView, ItemsView, ValuesView, MutableMapping

try:
    from thread import get_ident as _get_ident
except ImportError:
    from dummy_thread import get_ident as _get_ident
