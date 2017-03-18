# Setup Postgres

Here's the [postgres docs](http://www.postgresql.org/docs/9.3/interactive/), which can be useful for setting stuff up.

## Install Postgres

1. Install brew cask if you haven't already.

    * Install brew. Instructions [here](http://brew.sh/)
    * Install brew cask with: `brew install caskroom/cask/brew-cask`
    
    If you already have them, update: `brew update && brew upgrade brew-cask`

1. Install your Postgres database. The easiest way to to install the pre-build application (with an adorable icon) using the following command:

    ```
    brew cask install postgres
    ```

    If you don't have homebrew, go [here](http://brew.sh/).

2. After the installation is complete, use Spotlight to search for `Postgres` and open the Application. It will ask you if you want to move it to the Applications folder, say "Move it"


## Setting up psql on a Mac

1. Go to the home directory by running `cd` in the terminal

2. Open terminal configurations:

    * `nano ~/.bash_profile`

3. Insert the following line at the end of the file and save the file.

   ```export PATH=/Applications/Postgres.app/Contents/Versions/9.4/bin:$PATH``` 

4. Open a new terminal and run `psql`
