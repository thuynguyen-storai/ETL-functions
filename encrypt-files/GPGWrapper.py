import os
import gnupg
from gnupg import Crypt


class GPGWrapper:
    """A wrapper for `python-gnupg`
    """

    gpg = gnupg.GPG()

    @classmethod
    def import_from_config_name(cls, config_name: str) -> str:
        """Import key into GnuPG instance, through os.getenv()

        Args:
            config_name (str): _description_

        Raises:
            Exception: Config is missing or import failed

        Returns:
            str: The fingerprint of this key
        """
        config_value = os.getenv(config_name)
        if not config_value:
            raise Exception("Config is missing or have not declared")

        import_result: gnupg.ImportResult = cls.gpg.import_keys(config_value)
        return import_result.fingerprints[0]

    @classmethod
    def encrypt(cls, reader, recv_fingerprint: str, sender_fingerprint: str) -> str:
        encrypt_result: Crypt = cls.gpg.encrypt_file(reader,
                                                     recipients=recv_fingerprint,
                                                     always_trust=True,
                                                     armor=False,
                                                     extra_args=['--local-user', sender_fingerprint])
        if not encrypt_result.ok:
            raise Exception(f"Failed to encrypt file: {encrypt_result.problems}")

        return encrypt_result.data
