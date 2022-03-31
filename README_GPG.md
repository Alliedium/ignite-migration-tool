# Apache Ignite Migration Tool CI/CD GPG

Signed artifacts with GPG key is one of requirements to be able to publish artifacts to maven central repository.
While all information about this can be found in web for example [here](https://central.sonatype.org/publish/requirements/gpg/) 
this is a minimal instruction about GPG configuration for our repository.

Shortly the steps are:
1. Generate a GPG key
2. Send public key to key servers
3. Set repository secrets: passphrase and private key 

In order to generate a GPG key please follow this [instruction](https://help.ubuntu.com/community/GnuPrivacyGuardHowto).
After GPG key generation it's public key should be published to one of the following key servers:
 - keyserver.ubuntu.com
 - keys.openpgp.org
 - pgp.mit.edu

The following command will send the public key:
`gpg --keyserver keyserver.ubuntu.com --send-keys CA925CD6C9E8D064FF05B4728190C4130ABA0F98`
CA925CD6C9E8D064FF05B4728190C4130ABA0F98 is key id, it can be listed using `gpg --list-keys` command.
If the command fails to send GPG key, it can be done manually via browser https://keyserver.ubuntu.com/,
in such case use command `gpg -a --export CA925CD6C9E8D064FF05B4728190C4130ABA0F98`,
copy the public key and paste into https://keyserver.ubuntu.com/ .

After the GPG key will become available through key servers it is time to set repository secrets:
 - MAVEN_GPG_PASSPHRASE - passphrase of gpg key
 - MAVEN_GPG_PRIVATE_KEY - gpg private key

While with gpg passphrase all is obvious in order to get gpg private key the next command should be used:
`gpg --armor --export-secret-keys CA925CD6C9E8D064FF05B4728190C4130ABA0F98`
