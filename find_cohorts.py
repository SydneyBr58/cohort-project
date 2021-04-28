import pandas as pd
import numpy as np
from scipy.spatial import distance
import json

def closest_nodes(node, dfn):
    # returns the closest nodes from a point, from a given group of nodes
    nodes_left = dfn['nodes'].tolist()
    dist = distance.cdist([node], nodes_left)
    min_id = np.where(dist==dist.min())[1].tolist()
    if len(min_id)==1:
        return dfn.iloc[[min_id[0]], :].copy()         
    else:
        return dfn.iloc[min_id].copy()
    
    
def create_nodes_df():
    # Replace values by spatial coordinates based on index in list
    gen_repl_dict = {}
    df = pd.read_csv('avg_usage.csv')
    for col in ['bd', 'gender', 'registered_via']:
        list_col = df[col].drop_duplicates().sort_values().tolist()
        repl_dict = {key:list_col.index(key) for key in list_col}
        gen_repl_dict.update({col:repl_dict})
        df.loc[:,col] = df[col].replace(repl_dict)
    json.dump(gen_repl_dict, open('replacement_dict','w'))
    df['nodes'] = df.apply(lambda x: np.array([x['bd'], x['gender'], x['registered_via']]), axis=1)
    df['idx'] = df['bd'].map(str) + df['gender'].map(str) + df['registered_via'].map(str)
    df = df.set_index(['idx'])
    POP = df['nbr_users'].sum()
    df = df.sort_values(by='avg_usage', ascending=False)
    return df, POP


def add_max_node(dfn, df, group):
    # Add max node to group and remove it from nodes
    df = df[~df.index.duplicated(keep='first')]
    max_usage = df['avg_usage'].max()
    group = pd.concat([group, df[df['avg_usage']==max_usage]])
    dfn = dfn.drop(df[df['avg_usage']==max_usage].index)
    return dfn, group

def main():
    dfn, pop = create_nodes_df()
    df_final = pd.DataFrame()
    cpt = 0
    C = 5/100
    while len(dfn) > 0:
        group = dfn.iloc[[0], :]
        dfn = dfn.iloc[1:] #removes the element from the df
        while (group['nbr_users'].sum() <= C*POP) and (dfn['nbr_users'].sum() >= C*POP):
            df_pot_nodes = pd.DataFrame()
            for node in group['nodes']:
                close_nodes = closest_nodes(node, dfn)
                df_pot_nodes = pd.concat([df_pot_nodes, close_nodes])
            dfn, group = add_max_node(dfn, df_pot_nodes, group)
        cpt +=1
        if dfn['nbr_users'].sum() >= C*POP:
            group['nbr'] = cpt
            df_final = pd.concat([df_final, group])
        else:
            # Handles last group with remaining nodes
            group = group.append(dfn)
            dfn = pd.DataFrame()
            group['nbr'] = cpt
            df_final = pd.concat([df_final, group])