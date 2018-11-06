import os
import errno

def safe_mkdir(path, verbose=False):
    """
    Create a folder and catch the race condition between path exists and mkdir.

    Parameters
    ----------
    path : string
        Name of the folder to be created (can be full path).
    verbose : bool, optional
        If True, print messages about the status. Default is False.

    Examples
    ----------
    Folders are created
    >>> safe_mkdir('toto/titi/tutu', verbose=True)

    Folders aren't created because they already exist
    >>> safe_mkdir('toto/titi/tutu', verbose=True)
    Folders toto/titi/tutu already exist. Not created.

    >>> os.removedirs('toto/titi/tutu')
    """
    abspath = os.path.abspath(path)
    if not os.path.exists(abspath):
        try:
            os.makedirs(abspath)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
    else:
        if verbose:
            print("Folders {} already exist. Not created.".format(path))
