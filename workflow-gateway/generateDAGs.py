import sys
import random
def main():
    width = int(sys.argv[1])
    depth = int(sys.argv[2])
    
    # Generate Workflow
    workflow = [1]
    functionCounter = 1

    for x in range(1,depth-1):
        layer = random.randint(workflow[-1],width)
        workflow.append(layer)
        functionCounter+=layer
    workflow.append(width)
    functionCounter+=width
    template = open("workflows/image/utils/templateDAG.txt","r")
    templateContent = template.read()
    template.close()
    
    content = """
def function():
    task_params = {"""
    # appending all functions inside 
    for x in range(functionCounter):
        content+=f"""
        'function{x}': (100,5),"""
    content+="""
    }
    tasks = {}
    for task_id, params in task_params.items():
        tasks[task_id] = create_timed_task(task_id, *params)
    
    """
    # Generate workflow
    workflowString = ""
    functionDict = {}

    index = 0
    temp = ""
    # First layer
    for x in range(1,depth+1):
        temp += f"tasks['function{index}'] >>"
        if x in functionDict:
            functionDict[x].append(index)
        else:
            functionDict[x] = [index]
        index+=1
    workflowString+=temp[:-2]
    workflowString+="\n"
    layer = 2
    print(workflow)
    while layer <= depth:
        for x in range(workflow[layer-1]-len(functionDict[layer])):
            branchRoot = f"""
    tasks['function{(random.choice(functionDict[layer-1]))}'] >>"""
            flag = 0
            for y in range(layer,depth+1):
                flag = 1
                if y in functionDict:
                    functionDict[y].append(index)
                else:
                    functionDict[y] = [index]
                branchRoot+=f"tasks['function{index}'] >>"
                index+=1
            if flag == 1:
                workflowString+=branchRoot[:-2]
                workflowString+="\n"        
        layer+=1
            
    dst = open('workflows/image/airflow-dags/function.py','w')
    total = templateContent+content+workflowString+ "\netl_dag=function()"
    dst.write(total)
    dst.close()
if __name__ == "__main__":
    main()