import { Component, ElementRef, ViewChild } from '@angular/core';
import * as d3 from 'd3';
import { MatDialog } from '@angular/material/dialog';
import { CowDialogComponent } from '../cow-dialog/cow-dialog';

interface Cow {
  name: string;
  age?: number;
  gender?: 'male' | 'female';
  notes?: string;
  children?: Cow[];
}

let cows: Cow[] = [
  {
    name: 'Lilly',
    age: 11,
    gender: 'female'
  }
]; 

localStorage.setItem('cows', JSON.stringify(cows));

console.log(JSON.parse(localStorage.getItem('cows')));

@Component({
  selector: 'app-cattle-tree',
  imports: [ CowDialogComponent],
  templateUrl: './cattle-tree.html',
  styleUrl: './cattle-tree.scss'
})
export class CattleTree {
  @ViewChild('treeContainer', { static: true }) treeContainer!: ElementRef;

  private data: Cow = {
    name: 'Lilly',
    children: [
      { name: 'Bella' },
      { name: 'Daisy', children: [{ name: 'MooMoo' }, { name: 'Bessie' }] },
      { name: 'Buttercup' }
    ]
  };

  constructor(private dialog: MatDialog) {}

  ngOnInit(): void {
    this.createTree(this.data);
  }

  private createTree(data: Cow): void {
    const element = this.treeContainer.nativeElement;
    const width = 800;
    const height = 600;

    d3.select(element).selectAll('*').remove();

    const svg = d3.select(element)
      .append('svg')
      .attr('width', width)
      .attr('height', height)
      .append('g')
      .attr('transform', 'translate(50,50)');

    const root = d3.hierarchy(data);

    const treeLayout = d3.tree<Cow>().size([width - 100, height - 100]);
    treeLayout(root);

    // Draw links
    svg.selectAll('line.link')
      .data(root.links())
      .enter()
      .append('line')
      .attr('class', 'link')
      .attr('x1', d => d.source.x)
      .attr('y1', d => d.source.y)
      .attr('x2', d => d.target.x)
      .attr('y2', d => d.target.y)
      .attr('stroke', '#999')
      .attr('stroke-width', 2);

    // Draw nodes
    const node = svg.selectAll('g.node')
  .data(root.descendants())
  .enter()
  .append('g')
  .attr('class', 'node')
  .attr('transform', d => `translate(${d.x},${d.y})`)
  .call(d3.drag<SVGGElement, d3.HierarchyPointNode<Cow>>()
    .on('start', function (event, d) {
      d3.select(this).raise();
      d['dragging'] = true;
      d['dragStart'] = Date.now();
    })
    .on('drag', function (event, d) {
      d3.select(this).attr('transform', `translate(${event.x},${event.y})`);
    })
    .on('end', (event, d) => {
      const wasClick = Date.now() - d['dragStart'] < 200;
      d['dragging'] = false;

      if (wasClick) {
        // Open dialog only if it was a click (not drag)
        const cowData = d.data;
        this.dialog.open(CowDialogComponent, {
          data: cowData
        });
      }
    })
  );

    node.append('circle')
      .attr('r', 20)
      .attr('fill', '#89CFF0');

    node.append('text')
      .attr('dy', 5)
      .attr('text-anchor', 'middle')
      .text(d => d.data.name)
      .style('font-size', '12px');
  }
}
