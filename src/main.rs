use mongodb::{ Client, Collection, error::Error, options::InsertManyOptions};
use serde::*;

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::format;
use std::fs::{self, Metadata};
use std::io::{self, Bytes, Read};
use std::ops::Add;
use std::process::{exit, ExitCode};
use std::str::FromStr;
use std::{env::args, path::PathBuf};
use walkdir::WalkDir;
type array_string = Vec<String>;
type path_exit = Result<PathBuf, ExitCode>;
use std::thread::spawn;
#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
struct Infomation {
    host: String,
    path: String,
    user: String,
    pass: String,
    type_: String,
}

fn build_args() -> path_exit {
    let args: array_string = args().collect();
    if args.len() < 2 {
        eprintln!("Preciso do caminho");
        Err(exit(1))
    } else {
        let path: PathBuf = args[1].to_owned().into();
        Ok(path)
    }
}
fn exist_http(line: &str) -> bool {
    line.contains("http")
}

fn padrao_correto_split_dois_pontos_(vec: &Vec<&str>) -> bool {
    if vec.len() == 4 {
        true
    } else {
        false
    }
}

fn open_file(file: PathBuf) -> Result<Vec<Infomation>, io::Error> {
    
    let open = fs::read_to_string(file)?;
    let mut passwords = Vec::new();
    for linha in open.lines() {
        let split = linha.split(":").collect::<Vec<&str>>();
        if split.len() < 1 || !padrao_correto_split_dois_pontos_(&split) {
            continue;
        } else {
            if exist_http(split[0]) {
                let url = split[1].split("/").collect::<Vec<&str>>();

                if url.len() > 2 {
                    let host = url[2].to_string();
                    let type_split = host.split(".").collect::<Vec<&str>>();
                    if type_split.len() < 2 {
                        continue;
                    }
                    let type_ = type_split[type_split.len() - 1].to_string();
                    let path = url[3..].join("/");
                    let user = split[2].to_string();
                    let pass = split[3].to_string();
                    if user.len() < 2{
                        continue;
                    }
                    if pass.len() < 2{
                        continue;
                    }

                    passwords.push(Infomation {
                        host,
                        path,
                        user,
                        pass,
                        type_,
                    });
                }
            }
        }
    }
    Ok(passwords)
}
#[tokio::main]
async fn main() {
    let path = build_args().unwrap();

    let walkdir = WalkDir::new(path);
    let mut cont = 0;
    for files in walkdir.into_iter().filter_map(|e| e.ok()) {
        let files = files.clone();

        let file_path = files.into_path();
        if file_path.is_file() {
                    
            let open = open_file(file_path.clone());
            cont = 0;
            match open {
                Ok(v) => {
                    fs::remove_file(&file_path);
                    let mut VecInfo = v;
                    let remove_duplicadas = remove_duplicadas(VecInfo);
                    println!("Linhas a inserir: {}", remove_duplicadas.len());
                    //AdcionaAoBancoDeDadosMongoDb(remove_duplicadas).await;
                

                        AdcionaAoBancoDeDadosMongoDb(remove_duplicadas).await;
                   
                    
                }
             Err(error) => {

                println!("{}", error.to_string());
             }
            }
        } else {
        }
    }

  
}




fn remove_duplicadas(map: Vec<Infomation>)  -> HashSet<Infomation>{
    let mut hashnew = HashSet::new();
    for ma in map{
        hashnew.insert(ma);
    }
    hashnew
}

async fn AdcionaAoBancoDeDadosMongoDb(buffer: HashSet<Infomation>) -> Result<(), mongodb::error::Error>{
    // caso queira colocar o password é logo abaixo
   let client =  Client::with_uri_str("mongodb://localhost:27017").await?;
    let db = client.database("login");

    let mut hashmap: HashMap<String ,Vec<Infomation>> = HashMap::new();
    for object in buffer {
        let type_key = format!("contry_{}", object.type_);
        hashmap.entry(type_key).or_insert_with(Vec::new).push(object);
    }
    let mut valor_para_mostra_na_tela = 1000;
    let mut total = 0;
    for (key, value) in &hashmap {
        let NameCollection = key.to_string();
        let mut collection: Collection<Infomation> = db.collection(&NameCollection);
       
        for c in value{
            
              
            let insert = collection.insert_one(c.clone(), None).await;
            match insert {
                Ok(_) =>{
                    total +=1;
                    if total == valor_para_mostra_na_tela {
                        println!("Cheguei aos {}k inseridos , ultimo documento: {:?}", total, c);
                        valor_para_mostra_na_tela += 10000;
                       }
                                  }
                Err(_) => {

                }
            }
        }
        
    }

    println!("Total inserido: {}  , {} ", total , valor_para_mostra_na_tela);

    Ok(())
}
