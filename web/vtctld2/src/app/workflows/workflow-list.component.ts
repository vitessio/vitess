import { Component, OnInit} from '@angular/core';
import { Node, Action, Display, State, ActionState, ActionStyle } from './node';
import { WorkflowComponent } from './workflow.component';
import { Accordion, AccordionTab, Header } from 'primeng/primeng';
import { WorkflowService } from '../api/workflow.service';
@Component({
  selector: 'vt-tasks',
  templateUrl: './workflow-list.component.html',
  styleUrls: ['./workflow-list.component.css', './workflow.component.css'],
  directives: [
    WorkflowComponent,
    Accordion, AccordionTab, Header,
  ],
  providers: [WorkflowService],
})

export class TasksComponent implements OnInit {
  title = 'Vitess Control Panel';
  workflows = [
     new Node('Horizontal Resharding Workflow', '/UU130429', [
      new Node('Approval', '/UU130429/1', []),
      new Node('Bootstrap', '/UU130429/2', [
        new Node('Copy to -80', '/UU130429/2/6', []),
        new Node('Copy to 80-', '/UU130429/2/7', [])
      ]),
      new Node('Diff', '/UU130429/3', [
        new Node('Copy to -80', '/UU130429/3/9', []),
        new Node('Copy to 80-', '/UU130429/3/10', [])
      ]),
      new Node('Redirect', '/UU130429/4', [
        new Node('Redirect REPLICA', '/UU130429/4/11', [
          new Node('Redirect -80', '/UU130429/4/11/14', []),
          new Node('Redirect 80-', '/UU130429/4/11/15', []),
        ]),
        new Node('Redirect RDONLY', '/UU130429/4/12', [
          new Node('Redirect -80', '/UU130429/4/12/16', []),
          new Node('Redirect 80-', '/UU130429/4/12/17', []),
        ]),
        new Node('Redirect Master', '/UU130429/4/13', [
          new Node('Redirect -80', '/UU130429/4/13/16', []),
          new Node('Redirect 80-', '/UU130429/4/13/17', []),
        ]),
      ]),
      new Node('Cleanup', '/UU130429/5', [
        new Node('Redirect 80-', '/UU130429/5/18', []),
      ]),
    ])
  ];

  constructor(private workflowService: WorkflowService) {}

  ngOnInit() {
    this.workflowService.getWorkflows().subscribe(workflows => {
      this.processWorkflowJson(workflows);
    });
    // Resharding Workflow Example
    this.updateWorkFlow('/UU130429', {
                                      message: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt\
                                                ut labore et dolore magna aliqua.',
                                      state: State.RUNNING,
                                      lastChanged: 1471235000000,
                                      display: Display.DETERMINATE,
                                      progress: 63,
                                      progressMsg: '63%',
                                      disabled: false,
                                      log: 'Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque \
                                            laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi \
                                            architecto beatae vitae dicta sunt explicabo. Nemo enim ipsam voluptatem quia voluptas \
                                            sit aspernatur aut odit aut fugit, sed quia conseq/UUntur magni dolores eos qui ratione \
                                            voluptatem sequi nesciunt. Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet, \
                                            consectetur, adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et \
                                            dolore magnam aliquam quaerat voluptatem.'});
    this.updateWorkFlow('/UU130429/1', {
                                      message: `Workflow was not started automatically. Click 'Start'.`,
                                      state: State.DONE,
                                      lastChanged: 1471234131000,
                                      actions: [new Action('Start', ActionState.ENABLED, ActionStyle.TRIGGERED)],
                                      log: 'Started'});
    this.updateWorkFlow('/UU130429/2/6', {
                                      message: 'Copying data from 0',
                                      state: State.DONE,
                                      lastChanged: 1471234150000,
                                      display: Display.DETERMINATE,
                                      progress: 100,
                                      progressMsg: '56372/56372 rows',
                                      log: 'Success'});
    this.updateWorkFlow('/UU130429/2/7', {
                                      message: 'Copying data from 0',
                                      state: State.DONE,
                                      lastChanged: 1471234225000,
                                      display: Display.DETERMINATE,
                                      progress: 100,
                                      progressMsg: '56373/56373 rows',
                                      log: 'Success'});
    this.updateWorkFlow('/UU130429/2', {
                                      state: State.DONE,
                                      lastChanged: 1471234300000,
                                      display: Display.DETERMINATE,
                                      progress: 100,
                                      progressMsg: '2/2',
                                      actions: [
                                                new Action('Canary 1st Shard', ActionState.ENABLED, ActionStyle.TRIGGERED),
                                                new Action('Remaining Shards', ActionState.ENABLED, ActionStyle.TRIGGERED)],
                                      log: 'Success'});
    this.updateWorkFlow('/UU130429/3/9', {
                                      message: 'Comparing -80 and 0',
                                      state: State.DONE,
                                      lastChanged: 1471234350000,
                                      display: Display.DETERMINATE,
                                      progress: 100,
                                      progressMsg: '56372/56372 rows',
                                      log: 'Success'});
    this.updateWorkFlow('/UU130429/3/10', {
                                      message: 'Comparing 80- and 0',
                                      state: State.DONE,
                                      lastChanged: 1471234425000,
                                      display: Display.DETERMINATE,
                                      progress: 100,
                                      progressMsg: '56373/56373 rows',
                                      log: 'Success'});
    this.updateWorkFlow('/UU130429/3', {
                                      state: State.DONE,
                                      lastChanged: 1471234500000,
                                      display: Display.DETERMINATE,
                                      progress: 100,
                                      progressMsg: '2/2',
                                      actions: [
                                                new Action('Canary 1st Shard', ActionState.ENABLED, ActionStyle.TRIGGERED),
                                                new Action('Remaining Shards', ActionState.ENABLED, ActionStyle.TRIGGERED)],
                                      log: 'Success'});
    this.updateWorkFlow('/UU130429/4/11', {
                                      message: 'Migrating Serve Type: REPLICA',
                                      state: State.DONE,
                                      lastChanged: 1471234700000,
                                      display: Display.DETERMINATE,
                                      progress: 100,
                                      progressMsg: '2/2'});
    this.updateWorkFlow('/UU130429/4/11/14', {
                                      message: 'Migrating -80',
                                      state: State.DONE,
                                      lastChanged: 1471234650000,
                                      display: Display.DETERMINATE,
                                      progress: 100,
                                      log: 'Success on tablet 1 \nSuccess on tablet 2 \nSuccess on tablet 3 \nSuccess on tablet 4 \n'});
    this.updateWorkFlow('/UU130429/4/11/15', {
                                      message: 'Migrating 80-',
                                      state: State.DONE,
                                      lastChanged: 1471234700000,
                                      display: Display.DETERMINATE,
                                      progress: 100,
                                      log: 'Success on tablet 1 \nSuccess on tablet 2 \nSuccess on tablet 3 \nSuccess on tablet 4 \n'});
    this.updateWorkFlow('/UU130429/4/12', {
                                      message: 'Migrating Serve Type: RDONLY',
                                      state: State.RUNNING,
                                      lastChanged: 1471234800000,
                                      display: Display.DETERMINATE,
                                      progress: 50,
                                      progressMsg: '1/2'});
    this.updateWorkFlow('/UU130429/4/12/16', {
                                      message: 'Migrating -80',
                                      state: State.DONE,
                                      lastChanged: 1471234750000,
                                      display: Display.DETERMINATE,
                                      progress: 100,
                                      log: 'Success on tablet 1 \nSuccess on tablet 2 \nSuccess on tablet 3 \nSuccess on tablet 4 \n'});
    this.updateWorkFlow('/UU130429/4/12/17', {
                                      message: 'Migrating 80-',
                                      state: State.RUNNING,
                                      display: Display.DETERMINATE});
    this.updateWorkFlow('/UU130429/4/13', {
                                      message: 'Migrating Serve Type: MASTER',
                                      display: Display.DETERMINATE,
                                      progressMsg: '0/2'});
    this.updateWorkFlow('/UU130429/4/13/16', {
                                      message: 'Migrating -80',
                                      display: Display.DETERMINATE});
    this.updateWorkFlow('/UU130429/4/13/17', {
                                      message: 'Migrating 80-',
                                      display: Display.DETERMINATE});
    this.updateWorkFlow('/UU130429/4', {
                                      message: '',
                                      state: State.RUNNING,
                                      lastChanged: 1471235000000,
                                      display: Display.DETERMINATE,
                                      progress: 50,
                                      progressMsg: '3/6',
                                      actions: [
                                                new Action('Canary 1st Tablet Type', ActionState.ENABLED, ActionStyle.TRIGGERED),
                                                new Action('Remaining Tablet Types', ActionState.ENABLED, ActionStyle.NORMAL)]});
    this.updateWorkFlow('/UU130429/5/18', {
                                      message: 'Recursively removing old shards',
                                      display: Display.DETERMINATE});
  }

  processWorkflowJson(workflows: any) {
    console.log(workflows);
    // let workflowList = Object.keys(workflows);
    // Iterate over all workflows
    for (let workflowData of workflows) {
      if (workflowData.path.charAt(0) === '/' && workflowData.path.split('/').length === 2) {
        this.setRootWorkflow(workflowData.path.split('/')[1], this.recursiveWorkflowBuilder(workflowData));
      } else {
        // Even if the workflow is not a root workflow it's path must be absolute
        if (workflowData.path.charAt(0) === '/') {
          let target = this.getWorkflow(workflowData.path);
          // This node already exists, so we simply need to update its data.
          if (target) {
            // Now we update all fields, but children is filled with a JS object not a Node
            target.update(workflowData);
            target.children = [];
            for (let childData of workflowData.children) {
              let child = this.recursiveWorkflowBuilder(childData);
              if (child) {
                target.children.push(child);
              }
            }
          } else {
            console.error('Could not find node to update');
          }
        } else {
          console.error('The path provided was not absolute.');
        }
      }
      /*if (root) {
        // create new node on root and pass this new node as parent for recursive step
      } else {
        target = getWorkflow(path);
        if (target) {
          target.update(data.minus(children));
          // loop through children and assign the recursivbe workflow
          for (let child of workflow.children) {
            target.children[child.id] = recursiveWorkflowBuilder(child);
          }
        } else {
          parent = getWorkflow(path.parent);
          if(parent) {
            parent.children[workflow.id] = new Node(getParams(workflow));
            target = parent.children[workflow.id];
            target.update(Data.minus(children));
          } else {
            continue;
          }
        }
      }*/
    }
  }

  setRootWorkflow(rootId, workflow) {
    console.log('Would add', Node, 'to root');
  }

  recursiveWorkflowBuilder(workflowData): Node {
    if (workflowData.name && workflowData.path) {
      let workflow = new Node(workflowData.name, workflowData.path, []);
      workflow.update(workflowData);
      workflow.children = [];
      for (let childData of workflowData.children) {
        let child = this.recursiveWorkflowBuilder(childData);
        if (child) {
          workflow.children.push(child);
        }
      }
      return workflow;
    }
    return undefined;
  }

  getRootWorkflowIds() {
    return this.workflows;
  }

  getWorkflow(path, relativeWorkflow= undefined) {
    let target = relativeWorkflow;
    // Check is path is relative or absolute.
    if (path.length > 0 && path.charAt(0) === '/') {
      target = {children: this.workflows};
      path = path.substring(1);
    }

    // Clean any trailing slashes.
    if (path.length > 0 && path.charAt(path.length - 1) === '/') {
      path = path.substring(0, path.length - 1);
    }

    let tokens = path.split('/');
    for (let i = 0; i < tokens.length; i++) {
      if (target) {
        target = this.getChild(tokens[i], target.children);
      } else {
        return target;
      }
    }
    return target;
  }

  getChild(id, children) {
    for (let child of children) {
      if (child.getId === id) {
        return child;
      }
    }
    return undefined;
  }

  updateWorkFlow(path: string, changes) {
    let target = this.getWorkflow(path);
    if (target) {
      target.update(changes);
    }
  }

  getHeaderClass(state) {
    switch (state) {
      case 0:
        return 'vt-workflow-not-started-dark';
      case 1:
        return 'vt-workflow-running-dark';
      case 2:
        return 'vt-workflow-done-dark';
      default:
        return '';
    }
  }
}

